-module(thinwrap_llm_anthropic).
-moduledoc false.

%% Native adapter for the Anthropic Messages API (POST <base_url>/messages).
%%
%% Anthropic is structurally different from OpenAI Chat Completions —
%% different auth (x-api-key + anthropic-version), a top-level `system`, typed
%% content blocks, tool results carried inside a user turn, `stop_sequences`,
%% and Anthropic-shaped SSE events — so this is a full translation layer, not
%% an OpenAI passthrough. It emits the identical normalized result / stream
%% delta / connector error as every other connector.
%%
%% Not normalized here (surfaced via `raw` or `passthrough`, never emulated):
%% response_format (Anthropic has none — use tool-based structured output via
%% passthrough); reasoning CoT output (thinking blocks land in `raw`, and
%% verbatim in `provider_metadata` `anthropic.content` for replay); prompt
%% caching (cache_control via passthrough; cache read/write tokens are counted
%% in `usage.input_tokens`, the breakdown stays in `raw`).

-export([complete/2, stream/4]).

-define(DEFAULT_BASE_URL, <<"https://api.anthropic.com/v1">>).
-define(DEFAULT_VERSION, <<"2023-06-01">>).
%% Used when the caller sets no max_tokens (Anthropic requires one). Current
%% models always think and default to medium effort, so a smaller ceiling often
%% ends at `length` before any text; max_tokens caps output, it is not billed
%% as such. 16000 is Anthropic's recommended non-streaming default.
-define(DEFAULT_MAX_TOKENS, 16000).

-define(USAGE_KEYS, [
    <<"input_tokens">>,
    <<"cache_read_input_tokens">>,
    <<"cache_creation_input_tokens">>,
    <<"output_tokens">>
]).

-doc false.
-spec complete(thinwrap_llm:config(), thinwrap_llm:chat_input()) -> thinwrap_llm:chat_result().
complete(Config, Input) ->
    {Url, Body, Headers} = build_request(Config, Input, false),
    #{status := Status, headers := RespHeaders, body := RespBody} =
        thinwrap_llm_http:post_json(Config, Url, Body, Headers),
    Raw = thinwrap_llm_json:decode(RespBody),
    thinwrap_llm_util:ok_status(Status) orelse
        thinwrap_llm_error:fail(to_error(Status, RespHeaders, Raw, undefined)),
    %% A Messages response always carries a `content` array; without it this
    %% is not a result, and parsing it would fake an empty completion.
    case thinwrap_llm_json:list(thinwrap_llm_json:get(Raw, <<"content">>)) of
        undefined -> thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status));
        Content -> parse_result(Raw, Content, maps:get(model, Input))
    end.

-doc false.
-spec stream(
    thinwrap_llm:config(),
    thinwrap_llm:chat_input(),
    fun((thinwrap_llm:stream_delta(), Acc) -> Acc),
    Acc
) -> Acc.
stream(Config, Input, Fun, Acc0) ->
    {Url, Body, Headers} = build_request(Config, Input, true),
    Handler = #{
        error => fun(S, H, Raw) -> to_error(S, H, Raw, undefined) end,
        init => #{
            %% Usage is merged across events field by field, as the official
            %% SDK's MessageStream does: message_start reports the counts, and
            %% message_delta restates some of them cumulatively; a field it
            %% omits or sends as null keeps the earlier value.
            usage => #{},
            %% The response's content blocks, rebuilt from the events in block
            %% order so the terminal delta can carry them for verbatim replay
            %% (the same array a non-stream call returns).
            blocks => #{},
            partial => #{},
            %% A complete stream reports its stop_reason on message_delta and
            %% closes with message_stop; without either the answer was cut off.
            saw_stop => false,
            %% Anthropic indexes CONTENT BLOCKS (text + tool_use interleaved),
            %% so the first tool call can arrive at block index 1. The
            %% normalized index is the 0-based tool-call ordinal (matching the
            %% OpenAI-compat surface); each block index maps to its ordinal so
            %% the argument continuation deltas line up.
            next_tool => 0,
            tool_by_block => #{}
        },
        payload => fun on_event/3,
        finish => fun
            (_SawDone, _Meta, #{saw_stop := true}) ->
                [];
            (_SawDone, _Meta, _State) ->
                thinwrap_llm_error:fail(thinwrap_llm_error:stream_ended_early())
        end
    },
    thinwrap_llm_http:stream_sse(Config, Url, Body, Headers, Handler, Fun, Acc0).

on_event(Evt, #{status := S, headers := H}, St) ->
    case thinwrap_llm_json:str(thinwrap_llm_json:get(Evt, <<"type">>)) of
        %% An `event: error` frame after 200 OK must surface, not truncate. The
        %% HTTP status is 200, so classify from the error frame itself.
        <<"error">> ->
            thinwrap_llm_error:fail(to_error(S, H, Evt, stream_error_code(Evt)));
        <<"message_start">> ->
            {[], St#{
                usage := merge_usage(
                    maps:get(usage, St),
                    thinwrap_llm_json:get(thinwrap_llm_json:get(Evt, <<"message">>), <<"usage">>)
                )
            }};
        <<"content_block_start">> ->
            block_start(Evt, St);
        <<"content_block_delta">> ->
            block_delta(Evt, St);
        <<"message_delta">> ->
            message_delta(Evt, St);
        <<"message_stop">> ->
            {[], St#{saw_stop := true}};
        %% content_block_stop / ping → ignored
        _ ->
            {[], St}
    end.

block_start(
    Evt, #{blocks := Blocks, partial := Partial, next_tool := Next, tool_by_block := TB} = St
) ->
    Cb = thinwrap_llm_json:get(Evt, <<"content_block">>),
    Block = thinwrap_llm_json:int(thinwrap_llm_json:get(Evt, <<"index">>)),
    St1 =
        case Cb of
            #{} ->
                Partial1 =
                    case
                        maps:is_key(<<"input">>, Cb) orelse
                            ends_with(
                                thinwrap_llm_json:str(maps:get(<<"type">>, Cb, undefined)),
                                <<"tool_use">>
                            )
                    of
                        %% The input arrives as input_json_delta fragments.
                        true -> Partial#{Block => <<>>};
                        false -> Partial
                    end,
                St#{blocks := Blocks#{Block => Cb}, partial := Partial1};
            _ ->
                St
        end,
    case thinwrap_llm_json:str(thinwrap_llm_json:get(Cb, <<"type">>)) of
        <<"tool_use">> ->
            D0 = #{index => Next},
            D1 = thinwrap_llm_util:put_opt(
                id, thinwrap_llm_json:str(thinwrap_llm_json:get(Cb, <<"id">>)), D0
            ),
            D2 = thinwrap_llm_util:put_opt(
                function_name, thinwrap_llm_json:str(thinwrap_llm_json:get(Cb, <<"name">>)), D1
            ),
            {[#{tool_call_delta => D2, raw => Evt}], St1#{
                next_tool := Next + 1, tool_by_block := TB#{Block => Next}
            }};
        _ ->
            {[], St1}
    end.

block_delta(Evt, #{blocks := Blocks, partial := Partial, tool_by_block := TB} = St) ->
    D = thinwrap_llm_json:get(Evt, <<"delta">>),
    Block = thinwrap_llm_json:int(thinwrap_llm_json:get(Evt, <<"index">>)),
    {Blocks1, Partial1} = accumulate_block(Blocks, Partial, Block, D),
    St1 = St#{blocks := Blocks1, partial := Partial1},
    case thinwrap_llm_json:str(thinwrap_llm_json:get(D, <<"type">>)) of
        <<"text_delta">> ->
            case thinwrap_llm_json:str(thinwrap_llm_json:get(D, <<"text">>)) of
                <<>> -> {[], St1};
                T -> {[#{content_delta => T, raw => Evt}], St1}
            end;
        <<"input_json_delta">> ->
            %% Server-tool blocks (server_tool_use) stream their input the same
            %% way but are not caller tool calls.
            case {thinwrap_llm_json:get(D, <<"partial_json">>), TB} of
                {Pj, #{Block := Idx}} when is_binary(Pj) ->
                    Tcd = thinwrap_llm_util:put_opt(arguments_delta, Pj, #{index => Idx}),
                    {[#{tool_call_delta => Tcd, raw => Evt}], St1};
                _ ->
                    {[], St1}
            end;
        %% thinking_delta → CoT output not normalized; available via raw and
        %% the terminal provider_metadata.
        _ ->
            {[], St1}
    end.

message_delta(Evt, #{usage := Usage0, blocks := Blocks, partial := Partial} = St) ->
    Usage = merge_usage(Usage0, thinwrap_llm_json:get(Evt, <<"usage">>)),
    In = input_tokens(Usage),
    Out = thinwrap_llm_json:int(maps:get(<<"output_tokens">>, Usage, 0)),
    D0 = #{
        raw => Evt,
        usage => #{input_tokens => In, output_tokens => Out, total_tokens => In + Out}
    },
    St1 = St#{usage := Usage},
    case
        thinwrap_llm_json:str(
            thinwrap_llm_json:get(thinwrap_llm_json:get(Evt, <<"delta">>), <<"stop_reason">>)
        )
    of
        <<>> ->
            {[D0], St1};
        Sr ->
            %% Only the terminal delta carries the turn's blocks.
            D1 = D0#{
                finish_reason => map_stop_reason(Sr),
                provider_metadata => #{
                    <<"anthropic">> => #{<<"content">> => finish_blocks(Blocks, Partial)}
                }
            },
            {[D1], St1#{saw_stop := true}}
    end.

merge_usage(Fields, #{} = Usage) ->
    lists:foldl(
        fun(K, Acc) ->
            case maps:get(K, Usage, undefined) of
                V when is_number(V) -> Acc#{K => V};
                _ -> Acc
            end
        end,
        Fields,
        ?USAGE_KEYS
    );
merge_usage(Fields, _) ->
    Fields.

%% Every input token billed for the request. Anthropic reports cache reads and
%% cache writes apart from `input_tokens`; the normalized count includes them
%% so it means the same thing across providers.
input_tokens(Usage) ->
    trunc(
        thinwrap_llm_json:num(thinwrap_llm_json:get(Usage, <<"input_tokens">>)) +
            thinwrap_llm_json:num(thinwrap_llm_json:get(Usage, <<"cache_read_input_tokens">>)) +
            thinwrap_llm_json:num(thinwrap_llm_json:get(Usage, <<"cache_creation_input_tokens">>))
    ).

accumulate_block(Blocks, Partial, Index, Delta) ->
    case Blocks of
        #{Index := #{} = Block} ->
            case thinwrap_llm_json:str(thinwrap_llm_json:get(Delta, <<"type">>)) of
                <<"text_delta">> ->
                    {
                        Blocks#{
                            Index := append(
                                Block,
                                <<"text">>,
                                thinwrap_llm_json:str(thinwrap_llm_json:get(Delta, <<"text">>))
                            )
                        },
                        Partial
                    };
                <<"thinking_delta">> ->
                    {
                        Blocks#{
                            Index := append(
                                Block,
                                <<"thinking">>,
                                thinwrap_llm_json:str(thinwrap_llm_json:get(Delta, <<"thinking">>))
                            )
                        },
                        Partial
                    };
                <<"signature_delta">> ->
                    {
                        Blocks#{
                            Index := Block#{
                                <<"signature">> => thinwrap_llm_json:str(
                                    thinwrap_llm_json:get(Delta, <<"signature">>)
                                )
                            }
                        },
                        Partial
                    };
                <<"input_json_delta">> when is_map_key(Index, Partial) ->
                    Pj = thinwrap_llm_json:str(thinwrap_llm_json:get(Delta, <<"partial_json">>)),
                    {Blocks, Partial#{Index := <<(maps:get(Index, Partial))/binary, Pj/binary>>}};
                <<"citations_delta">> ->
                    {append_citation(Blocks, Index, Block, Delta), Partial};
                _ ->
                    {Blocks, Partial}
            end;
        _ ->
            {Blocks, Partial}
    end.

append_citation(Blocks, Index, Block, Delta) ->
    case thinwrap_llm_json:get(Delta, <<"citation">>) of
        undefined ->
            Blocks;
        Citation ->
            Citations =
                case maps:get(<<"citations">>, Block, undefined) of
                    L when is_list(L) -> L;
                    _ -> []
                end,
            Blocks#{Index := Block#{<<"citations">> => Citations ++ [Citation]}}
    end.

append(Block, Key, Text) ->
    Block#{Key => <<(thinwrap_llm_json:str(maps:get(Key, Block, undefined)))/binary, Text/binary>>}.

finish_blocks(Blocks, Partial) ->
    [
        case Partial of
            #{Index := Pj} when Pj =/= <<>> ->
                Block#{<<"input">> => safe_json_parse(Pj)};
            #{Index := _} ->
                case maps:get(<<"input">>, Block, undefined) of
                    #{} ->
                        Block;
                    %% No deltas followed: the input on content_block_start
                    %% (if any) is the whole input.
                    _ ->
                        Block#{<<"input">> => #{}}
                end;
            _ ->
                Block
        end
     || {Index, Block} <- lists:keysort(1, maps:to_list(Blocks))
    ].

build_request(Config, Input, Stream) ->
    #{model := Model, messages := Msgs, tools := Tools, stop := Stop} = Input,
    {System, Messages} = split_messages(Msgs),
    MaxTokens =
        case Input of
            #{max_output_tokens := M} ->
                M;
            _ ->
                case maps:get(default_max_tokens, Config, 0) of
                    D when D > 0 -> D;
                    _ -> ?DEFAULT_MAX_TOKENS
                end
        end,
    B0 = #{<<"model">> => Model, <<"max_tokens">> => MaxTokens, <<"messages">> => Messages},
    B1 =
        case System of
            <<>> -> B0;
            _ -> B0#{<<"system">> => System}
        end,
    B2 =
        case Tools of
            [] -> B1;
            _ -> B1#{<<"tools">> => [map_tool(T) || T <- Tools]}
        end,
    B3 =
        case Input of
            #{tool_choice := TC} -> B2#{<<"tool_choice">> => map_tool_choice(TC)};
            _ -> B2
        end,
    B4 =
        case Input of
            #{reasoning := #{effort := Effort}} ->
                %% Adaptive thinking is the current-generation control: the
                %% model sizes its own thinking from the effort, so there is no
                %% budget to fit under max_tokens. Claude 4.5 and older reject
                %% it with a 400; those models take budget thinking via
                %% passthrough. Thinking also rules out custom sampling, so
                %% temperature/top_p are omitted.
                B3#{
                    <<"thinking">> => #{<<"type">> => <<"adaptive">>},
                    %% An empty effort means "unspecified", so the documented
                    %% default applies; any other value is sent as given for
                    %% Anthropic to validate.
                    <<"output_config">> => #{
                        <<"effort">> => thinwrap_llm_util:first_non_empty([Effort, <<"medium">>])
                    }
                };
            _ ->
                opt(
                    <<"top_p">>,
                    maps:get(top_p, Input, undefined),
                    opt(<<"temperature">>, maps:get(temperature, Input, undefined), B3)
                )
        end,
    B5 =
        case Stop of
            [] -> B4;
            _ -> B4#{<<"stop_sequences">> => Stop}
        end,
    %% response_format is intentionally NOT mapped — Anthropic has no
    %% response_format; structured output is via tools / passthrough.
    B6 =
        case Stream of
            true -> B5#{<<"stream">> => true};
            false -> B5
        end,
    Body = thinwrap_llm_passthrough:apply_body(B6, Input),
    Url = thinwrap_llm_passthrough:append_query(<<(base_url(Config))/binary, "/messages">>, Input),
    {Url, Body, auth_headers(Config, Input)}.

opt(_K, undefined, M) -> M;
opt(K, V, M) -> M#{K => V}.

base_url(Config) ->
    rstrip_slash(
        thinwrap_llm_util:first_non_empty([maps:get(base_url, Config, <<>>), ?DEFAULT_BASE_URL])
    ).

rstrip_slash(<<>>) ->
    <<>>;
rstrip_slash(B) ->
    case binary:last(B) of
        $/ -> rstrip_slash(binary:part(B, 0, byte_size(B) - 1));
        _ -> B
    end.

auth_headers(Config, Input) ->
    Version = thinwrap_llm_util:first_non_empty([
        maps:get(anthropic_version, Config, <<>>), ?DEFAULT_VERSION
    ]),
    Base = #{
        <<"x-api-key">> => thinwrap_llm_input:secret(Config, api_key),
        <<"anthropic-version">> => Version
    },
    thinwrap_llm_passthrough:merge_headers(maps:merge(Base, maps:get(headers, Config, #{})), Input).

map_tool(#{name := Name} = T) ->
    T0 = #{<<"name">> => Name},
    T1 =
        case T of
            #{description := D} when D =/= <<>> -> T0#{<<"description">> => D};
            _ -> T0
        end,
    T1#{
        <<"input_schema">> =>
            maps:get(parameters, T, #{<<"type">> => <<"object">>, <<"properties">> => #{}})
    }.

map_tool_choice(#{mode := auto}) ->
    #{<<"type">> => <<"auto">>};
map_tool_choice(#{mode := required}) ->
    #{<<"type">> => <<"any">>};
map_tool_choice(#{mode := none}) ->
    #{<<"type">> => <<"none">>};
map_tool_choice(#{mode := function, function_name := Name}) ->
    #{<<"type">> => <<"tool">>, <<"name">> => Name}.

split_messages(Msgs) ->
    {System, RevOut, _Rebuilt} = lists:foldl(fun split_one/2, {<<>>, [], false}, Msgs),
    {System, lists:reverse(RevOut)}.

split_one(#{role := system} = M, {System, Out, Rebuilt}) ->
    Text = content_to_text(M),
    case System of
        <<>> -> {Text, Out, Rebuilt};
        _ -> {<<System/binary, "\n\n", Text/binary>>, Out, Rebuilt}
    end;
split_one(#{role := tool, tool_call_id := Id} = M, {System, Out, Rebuilt}) ->
    Block = #{
        <<"type">> => <<"tool_result">>,
        <<"tool_use_id">> => Id,
        <<"content">> => content_to_text(M)
    },
    case Out of
        [#{<<"role">> := <<"user">>, <<"content">> := Content} = Last | Rest] ->
            case is_tool_result_content(Content) of
                true -> {System, [Last#{<<"content">> := Content ++ [Block]} | Rest], Rebuilt};
                false -> {System, [user_turn(Block) | Out], Rebuilt}
            end;
        _ ->
            {System, [user_turn(Block) | Out], Rebuilt}
    end;
%% Anthropic invalidates every thinking block after a turn whose thinking was
%% edited or stripped, and accounts that enforce preserved thinking reject
%% replaying them with a 400. Once one assistant turn is rebuilt, every later
%% one is rebuilt too.
split_one(#{role := assistant} = M, {System, Out, Rebuilt}) ->
    Replay =
        case Rebuilt of
            true -> undefined;
            false -> verbatim_content(M)
        end,
    Content =
        case Replay of
            undefined -> map_content_blocks(M);
            _ -> Replay
        end,
    {System, [#{<<"role">> => <<"assistant">>, <<"content">> => Content} | Out],
        Rebuilt orelse Replay =:= undefined};
split_one(#{role := Role} = M, {System, Out, Rebuilt}) ->
    {System, [#{<<"role">> => atom_to_binary(Role), <<"content">> => map_content_blocks(M)} | Out],
        Rebuilt}.

user_turn(Block) -> #{<<"role">> => <<"user">>, <<"content">> => [Block]}.

%% The assistant turn exactly as Anthropic returned it, thinking blocks
%% included, when `provider_metadata` still describes this message: the same
%% tool_use ids in order with the same inputs, and the same text. A message
%% edited since (or built by hand, or from another provider) is rebuilt from
%% its normalized fields instead, which drops its thinking blocks; replaying
%% the stored blocks would silently undo the edit.
verbatim_content(#{tool_calls := ToolCalls} = M) ->
    Content =
        case M of
            #{provider_metadata := #{<<"anthropic">> := Meta}} ->
                thinwrap_llm_json:get(Meta, <<"content">>);
            _ ->
                undefined
        end,
    case Content of
        [_ | _] ->
            case lists:all(fun is_map/1, Content) of
                false -> undefined;
                true -> verbatim_if_unchanged(Content, ToolCalls, M)
            end;
        _ ->
            undefined
    end.

%% `Content` is a non-empty list of maps (checked by the caller).
verbatim_if_unchanged(Content, ToolCalls, M) ->
    ToolUses = [
        B
     || B <- Content, maps:get(<<"type">>, B, undefined) =:= <<"tool_use">>
    ],
    Ids = [
        thinwrap_llm_json:str(maps:get(<<"id">>, B, undefined))
     || B <- ToolUses
    ],
    case Ids =:= [Id || #{id := Id} <- ToolCalls] of
        false ->
            undefined;
        true ->
            Text = iolist_to_binary([
                thinwrap_llm_json:str(maps:get(<<"text">>, B, undefined))
             || B <- Content, maps:get(<<"type">>, B, undefined) =:= <<"text">>
            ]),
            case same_inputs(ToolUses, ToolCalls) andalso Text =:= content_to_text(M) of
                true -> Content;
                false -> undefined
            end
    end.

%% Arguments are compared as parsed JSON (deep-equal), so a reformatted but
%% equal arguments text still replays verbatim, and an edited one never does.
same_inputs(ToolUses, ToolCalls) ->
    lists:all(
        fun({Block, #{arguments := ArgsText}}) ->
            Args =
                case ArgsText of
                    <<>> -> #{};
                    _ -> thinwrap_llm_json:decode(ArgsText)
                end,
            Args =/= undefined andalso maps:get(<<"input">>, Block, null) == Args
        end,
        lists:zip(ToolUses, ToolCalls)
    ).

content_to_text(#{content_parts := [_ | _] = Parts}) ->
    iolist_to_binary([T || #{type := text, text := T} <- Parts]);
content_to_text(#{content := C}) ->
    C.

map_content_blocks(#{role := Role, tool_calls := ToolCalls} = M) ->
    Blocks0 =
        case M of
            #{content_parts := [_ | _] = Parts} ->
                lists:flatmap(fun map_part/1, Parts);
            #{content := C} when C =/= <<>> ->
                [#{<<"type">> => <<"text">>, <<"text">> => C}];
            _ ->
                []
        end,
    Blocks =
        case {Role, ToolCalls} of
            {assistant, [_ | _]} ->
                Blocks0 ++
                    [
                        #{
                            <<"type">> => <<"tool_use">>,
                            <<"id">> => Id,
                            <<"name">> => Name,
                            <<"input">> => safe_json_parse(Args)
                        }
                     || #{id := Id, name := Name, arguments := Args} <- ToolCalls
                    ];
            _ ->
                Blocks0
        end,
    case Blocks of
        [] -> [#{<<"type">> => <<"text">>, <<"text">> => <<>>}];
        _ -> Blocks
    end.

%% Anthropic rejects an empty text block, so an empty part is dropped.
map_part(#{type := text, text := <<>>}) ->
    [];
map_part(#{type := text, text := T}) ->
    [#{<<"type">> => <<"text">>, <<"text">> => T}];
map_part(#{media_type := MT, base64 := B64}) ->
    [
        #{
            <<"type">> => <<"image">>,
            <<"source">> => #{
                <<"type">> => <<"base64">>, <<"media_type">> => MT, <<"data">> => B64
            }
        }
    ].

is_tool_result_content([#{<<"type">> := <<"tool_result">>} | _]) -> true;
is_tool_result_content(_) -> false.

safe_json_parse(Text) ->
    case thinwrap_llm_json:decode(Text) of
        undefined -> #{};
        null -> #{};
        V -> V
    end.

parse_result(Raw, Content, RequestedModel) ->
    {TextParts, ToolCalls} = lists:foldr(
        fun(B, {Ts, Calls}) ->
            case thinwrap_llm_json:str(thinwrap_llm_json:get(B, <<"type">>)) of
                <<"text">> ->
                    {[thinwrap_llm_json:str(thinwrap_llm_json:get(B, <<"text">>)) | Ts], Calls};
                <<"tool_use">> ->
                    Inp =
                        case thinwrap_llm_json:get(B, <<"input">>) of
                            undefined -> #{};
                            I -> I
                        end,
                    Call = #{
                        id => thinwrap_llm_json:str(thinwrap_llm_json:get(B, <<"id">>)),
                        name => thinwrap_llm_json:str(thinwrap_llm_json:get(B, <<"name">>)),
                        arguments => thinwrap_llm_json:encode(Inp)
                    },
                    {Ts, [Call | Calls]};
                %% thinking blocks → raw + provider_metadata only
                _ ->
                    {Ts, Calls}
            end
        end,
        {[], []},
        Content
    ),
    Usage = thinwrap_llm_json:get(Raw, <<"usage">>),
    In = input_tokens(Usage),
    Out = thinwrap_llm_json:int(thinwrap_llm_json:get(Usage, <<"output_tokens">>)),
    #{
        message => #{
            role => assistant,
            content => iolist_to_binary(TextParts),
            tool_calls => ToolCalls,
            provider_metadata => #{<<"anthropic">> => #{<<"content">> => Content}}
        },
        finish_reason => map_stop_reason(
            thinwrap_llm_json:str(thinwrap_llm_json:get(Raw, <<"stop_reason">>))
        ),
        usage => #{input_tokens => In, output_tokens => Out, total_tokens => In + Out},
        model => thinwrap_llm_util:first_non_empty([
            thinwrap_llm_json:str(thinwrap_llm_json:get(Raw, <<"model">>)), RequestedModel
        ]),
        raw => Raw
    }.

to_error(Status, Headers, Raw, Code0) ->
    Msg = thinwrap_llm_util:first_non_empty([
        thinwrap_llm_json:error_message(Raw), <<"HTTP ", (integer_to_binary(Status))/binary>>
    ]),
    Code =
        case Code0 of
            undefined -> map_code(Status, Msg);
            _ -> Code0
        end,
    thinwrap_llm_error:vendor_error(Status, Headers, Raw, Code, Msg).

%% Classify an Anthropic `error` SSE frame delivered after HTTP 200 by its
%% error.type (the HTTP status is 200 and can't classify it).
stream_error_code(Evt) ->
    case
        string:lowercase(
            thinwrap_llm_json:str(
                thinwrap_llm_json:get(thinwrap_llm_json:get(Evt, <<"error">>), <<"type">>)
            )
        )
    of
        <<"rate_limit_error">> ->
            rate_limited;
        T when T =:= <<"authentication_error">>; T =:= <<"permission_error">> ->
            auth_failed;
        <<"invalid_request_error">> ->
            case is_context_overflow(thinwrap_llm_json:error_message(Evt)) of
                true -> context_length_exceeded;
                false -> invalid_request
            end;
        %% The streamed form of a 402; classified the same way.
        <<"billing_error">> ->
            rate_limited;
        T when
            T =:= <<"not_found_error">>; T =:= <<"request_too_large">>; T =:= <<"conflict_error">>
        ->
            invalid_request;
        T when T =:= <<"overloaded_error">>; T =:= <<"api_error">>; T =:= <<"timeout_error">> ->
            provider_unavailable;
        %% A frame without a recognizable type says nothing classifiable.
        _ ->
            unknown
    end.

%% Anthropic reports a context overflow as a plain invalid_request_error, so
%% only its own wording ("prompt is too long: N tokens > M maximum", "input
%% length and `max_tokens` exceed context limit") tells it apart. Looser
%% matches ("context", "max_tokens") also hit ordinary validation errors, and a
%% caller that trims history on this code would then loop forever.
is_context_overflow(Message) ->
    M = string:lowercase(Message),
    binary:match(M, <<"prompt is too long">>) =/= nomatch orelse
        binary:match(M, <<"exceed context limit">>) =/= nomatch.

map_stop_reason(Sr) when Sr =:= <<"end_turn">>; Sr =:= <<"stop_sequence">> -> stop;
%% model_context_window_exceeded: the output was cut off by the context window
%% rather than max_tokens; for the caller both mean "truncated".
map_stop_reason(Sr) when Sr =:= <<"max_tokens">>; Sr =:= <<"model_context_window_exceeded">> ->
    length;
map_stop_reason(<<"tool_use">>) ->
    tool_calls;
map_stop_reason(<<"refusal">>) ->
    content_filter;
%% pause_turn (a server-tool loop hit its iteration limit; send the turn back
%% to continue) has no normalized counterpart.
map_stop_reason(_) ->
    unknown.

map_code(S, _Msg) when S =:= 401; S =:= 403 ->
    auth_failed;
%% 402 means the account is out of credit or quota. OpenAI (429
%% insufficient_quota) and Gemini (RESOURCE_EXHAUSTED) already report that as
%% rate-limited, so 402 classifies the same way.
map_code(S, _Msg) when S =:= 402; S =:= 429 ->
    rate_limited;
%% Any 5xx is a provider-side failure, including gateway and Cloudflare-origin
%% statuses (504/520/522/524) a fixed whitelist would miss; 408 is a transient
%% timeout.
map_code(S, _Msg) when S >= 500; S =:= 408 ->
    provider_unavailable;
map_code(S, Msg) when S >= 400 ->
    case is_context_overflow(Msg) of
        true -> context_length_exceeded;
        %% 404, 413 request_too_large (a byte limit, not a token one), 422, …:
        %% the request cannot succeed as sent.
        false -> invalid_request
    end;
map_code(_, _) ->
    unknown.

ends_with(Bin, Suffix) ->
    Size = byte_size(Suffix),
    byte_size(Bin) >= Size andalso binary:part(Bin, byte_size(Bin) - Size, Size) =:= Suffix.
