-module(thinwrap_llm_openai_compat).
-moduledoc false.

%% The single connector shared by every first-class OpenAI-compatible provider
%% (`POST <base_url>/chat/completions`), parameterized by its row in
%% `thinwrap_llm_spec`. OpenAI Chat Completions is the canonical shape, so the
%% translation here is thin.

-export([complete/2, stream/4]).

%% Shared with the embeddings connector.
-export([base_url/2, auth_headers/2]).

-doc false.
-spec complete(thinwrap_llm:config(), thinwrap_llm:chat_input()) -> thinwrap_llm:chat_result().
complete(Config, Input) ->
    Spec = thinwrap_llm_spec:get(maps:get(provider, Config)),
    {Url, Body, Headers} = build_request(Spec, Config, Input, false),
    #{status := Status, headers := RespHeaders, body := RespBody} =
        thinwrap_llm_http:post_json(Config, Url, Body, Headers),
    Raw = thinwrap_llm_json:decode(RespBody),
    thinwrap_llm_util:ok_status(Status) orelse
        thinwrap_llm_error:fail(to_error(Status, RespHeaders, Raw, undefined)),
    Raw =:= undefined andalso thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status)),
    ok = check_success_body(Status, RespHeaders, Raw),
    parse_result(Raw, maps:get(model, Input)).

-doc false.
-spec stream(
    thinwrap_llm:config(),
    thinwrap_llm:chat_input(),
    fun((thinwrap_llm:stream_delta(), Acc) -> Acc),
    Acc
) -> Acc.
stream(Config, Input, Fun, Acc0) ->
    Spec = thinwrap_llm_spec:get(maps:get(provider, Config)),
    {Url, Body, Headers} = build_request(Spec, Config, Input, true),
    Handler = #{
        error => fun(S, H, Raw) -> to_error(S, H, Raw, undefined) end,
        %% A complete stream ends on [DONE], and its last choice chunk carries
        %% a finish_reason; either one proves the answer was not cut off.
        init => false,
        payload => fun(Chunk, Meta, SawFinish) -> on_chunk(Spec, Chunk, Meta, SawFinish) end,
        finish => fun
            (false, _Meta, false) ->
                thinwrap_llm_error:fail(thinwrap_llm_error:stream_ended_early());
            (_SawDone, _Meta, _SawFinish) ->
                []
        end
    },
    thinwrap_llm_http:stream_sse(Config, Url, Body, Headers, Handler, Fun, Acc0).

on_chunk(Spec, Chunk, #{status := S, headers := H}, SawFinish) ->
    %% A provider error emitted after 200 OK must not be swallowed as a silent
    %% truncation. The HTTP status is a success code, so the error object's
    %% own code or type classifies it.
    case is_map(Chunk) andalso thinwrap_llm_json:get(Chunk, <<"error">>) =/= undefined of
        true -> thinwrap_llm_error:fail(to_error(S, H, Chunk, stream_error_code(Chunk)));
        false -> ok
    end,
    Deltas = map_stream_chunk(Spec, Chunk),
    {Deltas, SawFinish orelse lists:any(fun(D) -> maps:is_key(finish_reason, D) end, Deltas)}.

-doc false.
-spec base_url(thinwrap_llm_spec:spec(), thinwrap_llm:config()) -> binary().
base_url(#{default_base_url := Default}, Config) ->
    Base =
        case maps:get(base_url, Config, <<>>) of
            <<>> -> Default;
            B -> B
        end,
    rstrip_slash(Base).

rstrip_slash(B) ->
    case B of
        <<>> ->
            B;
        _ ->
            case binary:last(B) of
                $/ -> rstrip_slash(binary:part(B, 0, byte_size(B) - 1));
                _ -> B
            end
    end.

-doc false.
%% Auth headers, then config headers, then passthrough headers (each later
%% source wins on an exact name).
-spec auth_headers(thinwrap_llm_spec:spec(), thinwrap_llm:config()) -> map().
auth_headers(#{auth := Auth}, Config) ->
    Base =
        case {Auth, thinwrap_llm_input:secret(Config, api_key)} of
            {_, <<>>} -> #{};
            {bearer, Key} -> #{<<"Authorization">> => <<"Bearer ", Key/binary>>};
            {azure, Key} -> #{<<"api-key">> => Key}
        end,
    maps:merge(Base, maps:get(headers, Config, #{})).

build_request(Spec, Config, Input, Stream) ->
    #{model := Model, messages := Msgs, tools := Tools, stop := Stop} = Input,
    ToolsAllowed = maps:get(supports_tools, Spec),
    B0 = #{<<"model">> => Model, <<"messages">> => [map_message(M) || M <- Msgs]},
    B1 =
        case ToolsAllowed andalso Tools =/= [] of
            true -> B0#{<<"tools">> => [map_tool(T) || T <- Tools]};
            false -> B0
        end,
    B2 =
        case {ToolsAllowed, Input} of
            {true, #{tool_choice := TC}} -> B1#{<<"tool_choice">> => map_tool_choice(TC)};
            _ -> B1
        end,
    B3 = opt(
        <<"response_format">>, map_response_format(maps:get(response_format, Input, undefined)), B2
    ),
    B4 = opt(<<"temperature">>, maps:get(temperature, Input, undefined), B3),
    B5 = opt(<<"top_p">>, maps:get(top_p, Input, undefined), B4),
    B6 = opt(maps:get(max_tokens_field, Spec), maps:get(max_output_tokens, Input, undefined), B5),
    B7 =
        case Stop of
            [] -> B6;
            [One] -> B6#{<<"stop">> => One};
            _ -> B6#{<<"stop">> => Stop}
        end,
    B8 =
        case Input of
            #{reasoning := #{effort := Effort}} -> B7#{<<"reasoning_effort">> => Effort};
            _ -> B7
        end,
    B9 =
        case {Stream, maps:get(strict_params, Spec)} of
            {false, _} ->
                B8;
            {true, true} ->
                B8#{<<"stream">> => true};
            {true, false} ->
                B8#{<<"stream">> => true, <<"stream_options">> => #{<<"include_usage">> => true}}
        end,
    Body = thinwrap_llm_passthrough:apply_body(B9, Input),
    Headers = thinwrap_llm_passthrough:merge_headers(auth_headers(Spec, Config), Input),
    Url = thinwrap_llm_passthrough:append_query(
        <<(base_url(Spec, Config))/binary, "/chat/completions">>, Input
    ),
    {Url, Body, Headers}.

opt(_K, undefined, M) -> M;
opt(K, V, M) -> M#{K => V}.

map_message(#{role := Role} = M) ->
    Out0 = #{<<"role">> => atom_to_binary(Role), <<"content">> => map_content(M)},
    Out1 =
        case M of
            #{name := N} when N =/= <<>> -> Out0#{<<"name">> => N};
            _ -> Out0
        end,
    Out2 =
        case M of
            #{tool_calls := [_ | _] = TCs} ->
                Out1#{
                    <<"tool_calls">> => [
                        #{
                            <<"id">> => Id,
                            <<"type">> => <<"function">>,
                            <<"function">> => #{<<"name">> => Name, <<"arguments">> => Args}
                        }
                     || #{id := Id, name := Name, arguments := Args} <- TCs
                    ]
                };
            _ ->
                Out1
        end,
    case M of
        #{tool_call_id := CallId} when CallId =/= <<>> -> Out2#{<<"tool_call_id">> => CallId};
        _ -> Out2
    end.

map_content(#{content_parts := [_ | _] = Parts}) ->
    [
        case P of
            #{type := text} ->
                #{<<"type">> => <<"text">>, <<"text">> => maps:get(text, P)};
            #{media_type := MT, base64 := B64} ->
                #{
                    <<"type">> => <<"image_url">>,
                    <<"image_url">> => #{
                        <<"url">> => <<"data:", MT/binary, ";base64,", B64/binary>>
                    }
                }
        end
     || P <- Parts
    ];
map_content(#{content := C}) when C =/= <<>> ->
    C;
map_content(#{role := assistant, tool_calls := [_ | _]}) ->
    null;
map_content(_) ->
    <<>>.

map_tool(#{name := Name} = T) ->
    Fn0 = #{<<"name">> => Name},
    Fn1 =
        case T of
            #{description := D} when D =/= <<>> -> Fn0#{<<"description">> => D};
            _ -> Fn0
        end,
    Fn2 =
        case T of
            #{parameters := P} -> Fn1#{<<"parameters">> => P};
            _ -> Fn1
        end,
    #{<<"type">> => <<"function">>, <<"function">> => Fn2}.

map_tool_choice(#{mode := function, function_name := Name}) ->
    #{<<"type">> => <<"function">>, <<"function">> => #{<<"name">> => Name}};
map_tool_choice(#{mode := Mode}) ->
    atom_to_binary(Mode).

map_response_format(undefined) ->
    undefined;
map_response_format(#{type := json_schema} = RF) ->
    #{
        <<"type">> => <<"json_schema">>,
        <<"json_schema">> => #{
            <<"name">> => maps:get(json_schema_name, RF),
            <<"schema">> => maps:get(json_schema, RF, null)
        }
    };
map_response_format(#{type := Type}) ->
    #{<<"type">> => atom_to_binary(Type)}.

%% A 2xx is not proof of success: OpenRouter reports a failure after
%% processing started as HTTP 200 with either only an `error` object, or
%% `choices[0]` carrying `finish_reason: "error"` + `error` beside partial
%% content. Without a choice there is no result to return at all.
check_success_body(Status, Headers, Raw) ->
    case thinwrap_llm_json:get(Raw, <<"error">>) of
        undefined -> ok;
        Err -> thinwrap_llm_error:fail(embedded_error(Status, Headers, Raw, Err))
    end,
    case thinwrap_llm_json:list(thinwrap_llm_json:get(Raw, <<"choices">>)) of
        undefined ->
            thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status));
        [] ->
            thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status));
        [Choice | _] ->
            ChoiceErr = thinwrap_llm_json:get(Choice, <<"error">>),
            case
                ChoiceErr =/= undefined orelse
                    thinwrap_llm_json:str(thinwrap_llm_json:get(Choice, <<"finish_reason">>)) =:=
                        <<"error">>
            of
                true -> thinwrap_llm_error:fail(embedded_error(Status, Headers, Raw, ChoiceErr));
                false -> ok
            end
    end.

embedded_error(Status, Headers, Raw, Err) ->
    Wrapped = #{<<"error">> => null_if_undefined(Err)},
    Msg = thinwrap_llm_json:error_message(Wrapped),
    %% The status is the one the response arrived with (the 2xx), as for a
    %% mid-stream frame; the embedded code or type only classifies. Without
    %% one the error says nothing classifiable.
    Code =
        case thinwrap_llm_error:embedded_error_class(Err) of
            undefined -> unknown;
            Effective -> map_code(Effective, Wrapped)
        end,
    thinwrap_llm_error:vendor_error(Status, Headers, Raw, Code, Msg).

null_if_undefined(undefined) -> null;
null_if_undefined(V) -> V.

parse_result(Raw, RequestedModel) ->
    Choice = first_choice(Raw),
    M = thinwrap_llm_json:get(Choice, <<"message">>),
    ToolCalls = [
        parse_tool_call(TC)
     || TC <- list_or_empty(thinwrap_llm_json:get(M, <<"tool_calls">>))
    ],
    #{
        message => #{
            role => assistant,
            content => content_text(thinwrap_llm_json:get(M, <<"content">>)),
            tool_calls => ToolCalls
        },
        finish_reason => map_finish_reason(
            thinwrap_llm_json:str(thinwrap_llm_json:get(Choice, <<"finish_reason">>))
        ),
        usage => map_usage(thinwrap_llm_json:get(Raw, <<"usage">>)),
        model => thinwrap_llm_util:first_non_empty([
            thinwrap_llm_json:str(thinwrap_llm_json:get(Raw, <<"model">>)), RequestedModel
        ]),
        raw => Raw
    }.

%% Only the first choice is normalized; any others (a passthrough `n > 1`)
%% stay in `raw`.
first_choice(Json) ->
    case thinwrap_llm_json:list(thinwrap_llm_json:get(Json, <<"choices">>)) of
        [C | _] -> C;
        _ -> undefined
    end.

list_or_empty(L) when is_list(L) -> L;
list_or_empty(_) -> [].

parse_tool_call(TC) ->
    Fn = thinwrap_llm_json:get(TC, <<"function">>),
    #{
        id => thinwrap_llm_json:str(thinwrap_llm_json:get(TC, <<"id">>)),
        name => thinwrap_llm_json:str(thinwrap_llm_json:get(Fn, <<"name">>)),
        arguments => thinwrap_llm_json:str(thinwrap_llm_json:get(Fn, <<"arguments">>))
    }.

map_stream_chunk(Spec, Chunk) ->
    Choice = first_choice(Chunk),
    Delta = thinwrap_llm_json:get(Choice, <<"delta">>),
    Base0 = thinwrap_llm_util:put_opt(
        content_delta, content_text(thinwrap_llm_json:get(Delta, <<"content">>)), #{}
    ),
    Base1 =
        case thinwrap_llm_json:str(thinwrap_llm_json:get(Choice, <<"finish_reason">>)) of
            <<>> -> Base0;
            FR -> Base0#{finish_reason => map_finish_reason(FR)}
        end,
    UsageRaw =
        case {thinwrap_llm_json:get(Chunk, <<"usage">>), maps:get(stream_usage_path, Spec)} of
            {undefined, x_groq} ->
                thinwrap_llm_json:get(thinwrap_llm_json:get(Chunk, <<"x_groq">>), <<"usage">>);
            {U, _} ->
                U
        end,
    Base2 =
        case UsageRaw of
            undefined -> Base1;
            _ -> Base1#{usage => map_usage(UsageRaw)}
        end,
    %% A chunk may carry several tool_calls entries at once — one delta each
    %% (the first rides on the base delta), keyed by the vendor index.
    TCDs = [
        tool_call_delta(TC)
     || TC <- list_or_empty(thinwrap_llm_json:get(Delta, <<"tool_calls">>))
    ],
    case {TCDs, map_size(Base2)} of
        {[], 0} ->
            [];
        {[], _} ->
            [Base2#{raw => Chunk}];
        {[First | Rest], _} ->
            [Base2#{tool_call_delta => First, raw => Chunk}] ++
                [#{tool_call_delta => T, raw => Chunk} || T <- Rest]
    end.

tool_call_delta(TC) ->
    Fn = thinwrap_llm_json:get(TC, <<"function">>),
    D0 = #{index => thinwrap_llm_json:int(thinwrap_llm_json:get(TC, <<"index">>))},
    D1 = thinwrap_llm_util:put_opt(
        id, thinwrap_llm_json:str(thinwrap_llm_json:get(TC, <<"id">>)), D0
    ),
    D2 = thinwrap_llm_util:put_opt(
        function_name, thinwrap_llm_json:str(thinwrap_llm_json:get(Fn, <<"name">>)), D1
    ),
    thinwrap_llm_util:put_opt(
        arguments_delta, thinwrap_llm_json:str(thinwrap_llm_json:get(Fn, <<"arguments">>)), D2
    ).

to_error(Status, Headers, Raw, undefined) ->
    to_error(Status, Headers, Raw, map_code(Status, Raw));
to_error(Status, Headers, Raw, Code) ->
    thinwrap_llm_error:vendor_error(
        Status, Headers, Raw, Code, thinwrap_llm_json:error_message(Raw)
    ).

%% Classify a provider error object delivered mid-stream (after HTTP 200). The
%% stream's status is a success code, so the frame's own `code` / `type`
%% decides (an HTTP status, or a name such as `insufficient_quota`). A frame
%% with neither says nothing classifiable, so it is `unknown`.
stream_error_code(Body) ->
    case thinwrap_llm_error:embedded_error_class(thinwrap_llm_json:get(Body, <<"error">>)) of
        undefined -> unknown;
        Effective -> map_code(Effective, Body)
    end.

%% Assistant text from `message.content` / `delta.content`. Mistral reasoning
%% models return a list of chunks instead of a string — `thinking` chunks plus
%% `{"type":"text","text":...}` answer chunks; only the answer is content (the
%% reasoning stays visible on `raw`).
content_text(C) when is_binary(C) ->
    C;
content_text(L) when is_list(L) ->
    iolist_to_binary([
        thinwrap_llm_json:str(thinwrap_llm_json:get(P, <<"text">>))
     || P <- L, thinwrap_llm_json:get(P, <<"type">>) =:= <<"text">>
    ]);
content_text(_) ->
    <<>>.

map_finish_reason(<<"stop">>) -> stop;
%% Together AI reports natural end-of-sequence as "eos" rather than "stop".
map_finish_reason(<<"eos">>) -> stop;
map_finish_reason(<<"length">>) -> length;
map_finish_reason(<<"tool_calls">>) -> tool_calls;
map_finish_reason(<<"function_call">>) -> tool_calls;
map_finish_reason(<<"content_filter">>) -> content_filter;
map_finish_reason(_) -> unknown.

map_usage(U) ->
    In = thinwrap_llm_json:int(thinwrap_llm_json:get(U, <<"prompt_tokens">>)),
    Out = thinwrap_llm_json:int(thinwrap_llm_json:get(U, <<"completion_tokens">>)),
    Total =
        case thinwrap_llm_json:int(thinwrap_llm_json:get(U, <<"total_tokens">>)) of
            0 -> In + Out;
            T -> T
        end,
    #{input_tokens => In, output_tokens => Out, total_tokens => Total}.

map_code(S, _Body) when S =:= 401; S =:= 403 ->
    auth_failed;
%% 402 (out of credits/balance on OpenRouter, DeepSeek, ...): OpenAI reports the
%% same condition as 429 insufficient_quota, so it classifies the same way.
map_code(S, _Body) when S =:= 402; S =:= 429 ->
    rate_limited;
%% 408 is a timeout on the provider's side; a retry can succeed.
map_code(S, _Body) when S >= 500; S =:= 408 ->
    provider_unavailable;
map_code(S, Body) when S >= 400 ->
    case is_context_overflow(Body) of
        true ->
            context_length_exceeded;
        false ->
            case is_content_filter(thinwrap_llm_json:error_message(Body)) of
                true -> content_filtered;
                %% 404, 413, 422, …: the request cannot succeed as sent.
                false -> invalid_request
            end
    end;
map_code(_, _) ->
    unknown.

%% OpenAI's documented code first; other compat servers only word it.
is_context_overflow(Body) ->
    thinwrap_llm_json:get(thinwrap_llm_json:get(Body, <<"error">>), <<"code">>) =:=
        <<"context_length_exceeded">> orelse
        has_any(string:lowercase(thinwrap_llm_json:error_message(Body)), [
            <<"context length">>, <<"maximum context">>, <<"context_length">>
        ]).

%% OpenAI words a moderation refusal as content_policy_violation; Azure and
%% others say "content filter"/"content_filter" or name the moderation step.
is_content_filter(Message) ->
    M = string:lowercase(Message),
    has_any(M, [<<"content_policy">>, <<"content policy">>, <<"moderation">>]) orelse
        (has_any(M, [<<"content">>]) andalso has_any(M, [<<"filter">>])).

has_any(Hay, Needles) -> lists:any(fun(N) -> binary:match(Hay, N) =/= nomatch end, Needles).
