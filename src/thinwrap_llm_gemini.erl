-module(thinwrap_llm_gemini).
-moduledoc false.

%% Native adapter for the Google Gemini (AI Studio) generateContent API.
%%
%% Gemini uses contents[]/parts[] (roles user/model), a top-level
%% systemInstruction, functionDeclarations, and keys tool results by function
%% NAME (not id). This connector translates the normalized surface to that
%% shape and back. Unlike Bedrock, Gemini streaming IS SSE
%% (`:streamGenerateContent?alt=sse`), so streaming is truly incremental.
%%
%% Gemini 3 rejects a replayed functionCall without its `thoughtSignature`, so
%% the signature rides on the tool call's `provider_metadata` under
%% `<<"gemini">>` and is put back on the functionCall part when the call is
%% replayed.

-export([complete/2, stream/4]).

-define(DEFAULT_BASE_URL, <<"https://generativelanguage.googleapis.com/v1beta">>).

%% Google's documented value for a functionCall whose real signature is not
%% available (a hand-built history, or one from another provider): it skips the
%% signature check instead of failing the request with a 400.
-define(SKIP_SIGNATURE, <<"skip_thought_signature_validator">>).

-doc false.
-spec complete(thinwrap_llm:config(), thinwrap_llm:chat_input()) -> thinwrap_llm:chat_result().
complete(Config, #{model := Model} = Input) ->
    Url = thinwrap_llm_passthrough:append_query(
        <<
            (base_url(Config))/binary,
            "/models/",
            (thinwrap_llm_util:rfc3986(Model))/binary,
            ":generateContent"
        >>,
        Input
    ),
    #{status := Status, headers := RespHeaders, body := RespBody} =
        thinwrap_llm_http:post_json(
            Config, Url, build_request(Config, Input), auth_headers(Config, Input)
        ),
    Raw = thinwrap_llm_json:decode(RespBody),
    thinwrap_llm_util:ok_status(Status) orelse
        thinwrap_llm_error:fail(to_error(Status, RespHeaders, Raw, undefined)),
    %% `candidates` may only be absent on a prompt-level block (reported via
    %% promptFeedback.blockReason, mapped to content_filter); otherwise its
    %% absence means there is no result to return.
    case no_result(Raw) of
        true -> thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status));
        false -> parse_result(Raw, Model)
    end.

-doc false.
-spec stream(
    thinwrap_llm:config(),
    thinwrap_llm:chat_input(),
    fun((thinwrap_llm:stream_delta(), Acc) -> Acc),
    Acc
) -> Acc.
stream(Config, #{model := Model} = Input, Fun, Acc0) ->
    Url = thinwrap_llm_passthrough:append_query(
        <<
            (base_url(Config))/binary,
            "/models/",
            (thinwrap_llm_util:rfc3986(Model))/binary,
            ":streamGenerateContent?alt=sse"
        >>,
        Input
    ),
    Handler = #{
        error => fun(S, H, Raw) -> to_error(S, H, Raw, undefined) end,
        init => #{
            saw_function_call => false,
            tool_index => 0,
            %% usageMetadata is cumulative (each chunk repeats the running
            %% totals), so only the last one is reported, once, on the terminal
            %% delta; a consumer summing per-chunk usage would otherwise
            %% over-count.
            finish => undefined,
            usage => undefined,
            tail_raw => null,
            %% A complete stream ends on a chunk with a finishReason (or a
            %% prompt-level block); without one the answer was cut off.
            saw_finish => false
        },
        payload => fun on_chunk/3,
        %% Gemini sends no `[DONE]`: the end is judged from the stream state alone.
        finish => fun(_SawDone, _Meta, St) -> on_end(St) end
    },
    thinwrap_llm_http:stream_sse(
        Config, Url, build_request(Config, Input), auth_headers(Config, Input), Handler, Fun, Acc0
    ).

on_chunk(Chunk, #{status := S, headers := H}, St0) ->
    %% A provider error emitted after 200 OK must surface, not truncate. The
    %% HTTP status is 200, so classify from the error body itself.
    case is_map(Chunk) andalso thinwrap_llm_json:get(Chunk, <<"error">>) =/= undefined of
        true -> thinwrap_llm_error:fail(to_error(S, H, Chunk, stream_error_code(Chunk)));
        false -> ok
    end,
    Cand = first_candidate(Chunk),
    case Cand =:= undefined andalso block_reason(Chunk) =/= <<>> of
        true ->
            %% A prompt-level safety block arrives as a chunk with no candidate
            %% and only promptFeedback.blockReason — a content_filter finish, so
            %% the consumer sees the block rather than a silent, empty end.
            {[#{raw => Chunk, finish_reason => content_filter}], St0#{saw_finish := true}};
        false ->
            Parts = candidate_parts(Cand),
            {Text, ToolDeltas, St1} = stream_parts(Parts, Chunk, St0, <<>>, []),
            %% Text precedes the tool-call deltas (Gemini emits prose first).
            Deltas =
                case Text of
                    <<>> -> ToolDeltas;
                    _ -> [#{raw => Chunk, content_delta => Text} | ToolDeltas]
                end,
            St2 =
                case thinwrap_llm_json:str(thinwrap_llm_json:get(Cand, <<"finishReason">>)) of
                    <<>> ->
                        St1;
                    FR ->
                        Finish =
                            case maps:get(saw_function_call, St1) of
                                true -> tool_calls;
                                false -> map_finish_reason(FR)
                            end,
                        St1#{finish := Finish, tail_raw := Chunk}
                end,
            St3 =
                case thinwrap_llm_json:get(Chunk, <<"usageMetadata">>) of
                    undefined -> St2;
                    U -> St2#{usage := map_usage(U), tail_raw := Chunk}
                end,
            {Deltas, St3}
    end.

stream_parts([], _Chunk, St, Text, Acc) ->
    {Text, lists:reverse(Acc), St};
stream_parts([P | Rest], Chunk, St, Text, Acc) ->
    %% Reasoning/CoT parts (thought=true) are not normalized into content —
    %% they surface only via `raw` (mirrors Anthropic).
    case thinwrap_llm_json:truthy(thinwrap_llm_json:get(P, <<"thought">>)) of
        true ->
            stream_parts(Rest, Chunk, St, Text, Acc);
        false ->
            case thinwrap_llm_json:get(P, <<"text">>) of
                T when is_binary(T), T =/= <<>> ->
                    stream_parts(Rest, Chunk, St, <<Text/binary, T/binary>>, Acc);
                _ ->
                    case thinwrap_llm_json:get(P, <<"functionCall">>) of
                        undefined ->
                            stream_parts(Rest, Chunk, St, Text, Acc);
                        FC ->
                            #{tool_index := I} = St,
                            %% The synthetic id is index-based so parallel calls
                            %% to the same function stay distinct; each
                            %% functionCall part is its own delta with a distinct
                            %% index so calls are never merged.
                            Id = thinwrap_llm_util:first_non_empty([
                                thinwrap_llm_json:str(thinwrap_llm_json:get(FC, <<"id">>)),
                                synthetic_id(I)
                            ]),
                            TCD0 = #{index => I, id => Id},
                            TCD1 = thinwrap_llm_util:put_opt(
                                function_name,
                                thinwrap_llm_json:str(thinwrap_llm_json:get(FC, <<"name">>)),
                                TCD0
                            ),
                            TCD2 = thinwrap_llm_util:put_opt(
                                arguments_delta,
                                stringify_args(thinwrap_llm_json:get(FC, <<"args">>)),
                                TCD1
                            ),
                            TCD = thinwrap_llm_util:put_opt(
                                provider_metadata, signature_metadata(P), TCD2
                            ),
                            stream_parts(
                                Rest,
                                Chunk,
                                St#{saw_function_call := true, tool_index := I + 1},
                                Text,
                                [#{raw => Chunk, tool_call_delta => TCD} | Acc]
                            )
                    end
            end
    end.

on_end(#{finish := Finish, usage := Usage, tail_raw := Tail} = St) ->
    Terminal =
        case Finish =:= undefined andalso Usage =:= undefined of
            true ->
                [];
            false ->
                [
                    thinwrap_llm_util:put_opt(
                        usage,
                        Usage,
                        thinwrap_llm_util:put_opt(finish_reason, Finish, #{raw => Tail})
                    )
                ]
        end,
    case Finish =:= undefined andalso not maps:get(saw_finish, St) of
        true -> {Terminal, thinwrap_llm_error:stream_ended_early()};
        false -> Terminal
    end.

base_url(Config) ->
    Base =
        case maps:get(base_url, Config, <<>>) of
            <<>> -> ?DEFAULT_BASE_URL;
            B -> B
        end,
    rstrip_slash(Base).

rstrip_slash(<<>>) ->
    <<>>;
rstrip_slash(B) ->
    case binary:last(B) of
        $/ -> rstrip_slash(binary:part(B, 0, byte_size(B) - 1));
        _ -> B
    end.

auth_headers(Config, Input) ->
    H = maps:merge(
        #{<<"x-goog-api-key">> => thinwrap_llm_input:secret(Config, api_key)},
        maps:get(headers, Config, #{})
    ),
    thinwrap_llm_passthrough:merge_headers(H, Input).

build_request(Config, #{messages := Msgs, tools := Tools, stop := Stop} = Input) ->
    {SystemInstruction, Contents} = split_messages(Msgs),
    B0 = #{<<"contents">> => Contents},
    B1 =
        case SystemInstruction of
            undefined -> B0;
            _ -> B0#{<<"systemInstruction">> => SystemInstruction}
        end,
    B2 =
        case Tools of
            [] ->
                B1;
            _ ->
                Decls = [function_declaration(T) || T <- Tools],
                WithTools = B1#{<<"tools">> => [#{<<"functionDeclarations">> => Decls}]},
                case function_calling_config(maps:get(tool_choice, Input, undefined)) of
                    undefined ->
                        WithTools;
                    FC ->
                        WithTools#{<<"toolConfig">> => #{<<"functionCallingConfig">> => FC}}
                end
        end,
    G0 =
        case {Input, maps:get(default_max_tokens, Config, 0)} of
            {#{max_output_tokens := M}, _} -> #{<<"maxOutputTokens">> => M};
            {_, D} when D > 0 -> #{<<"maxOutputTokens">> => D};
            _ -> #{}
        end,
    G1 = opt(<<"temperature">>, maps:get(temperature, Input, undefined), G0),
    G2 = opt(<<"topP">>, maps:get(top_p, Input, undefined), G1),
    G3 =
        case Stop of
            [] -> G2;
            _ -> G2#{<<"stopSequences">> => Stop}
        end,
    G4 = apply_response_format(G3, maps:get(response_format, Input, undefined)),
    G5 =
        case Input of
            %% thinkingBudget is still accepted by Gemini 3; thinkingLevel is the
            %% newer control and belongs with the Interactions API move.
            #{reasoning := #{effort := Effort}} ->
                G4#{<<"thinkingConfig">> => #{<<"thinkingBudget">> => effort_budget(Effort)}};
            _ ->
                G4
        end,
    B3 =
        case map_size(G5) of
            0 -> B2;
            _ -> B2#{<<"generationConfig">> => G5}
        end,
    thinwrap_llm_passthrough:apply_body(B3, Input).

opt(_K, undefined, M) -> M;
opt(K, V, M) -> M#{K => V}.

function_declaration(#{name := Name} = T) ->
    Fn0 = #{<<"name">> => Name},
    Fn1 =
        case T of
            #{description := D} when D =/= <<>> -> Fn0#{<<"description">> => D};
            _ -> Fn0
        end,
    case T of
        %% parametersJsonSchema takes JSON Schema as written; the older
        %% `parameters` is an OpenAPI subset that rejects keywords such as
        %% additionalProperties with a 400.
        #{parameters := P} -> Fn1#{<<"parametersJsonSchema">> => P};
        _ -> Fn1
    end.

%% Fold system messages into a single systemInstruction and map the rest into
%% Gemini's contents[]. Tool-result messages become functionResponse parts
%% keyed by function name and are batched onto a trailing user turn (matching
%% how Gemini expects parallel tool results).
split_messages(Msgs) ->
    %% The id→name map is filled while the history is walked, so a tool result
    %% resolves against the assistant turn before it: synthetic ids repeat from
    %% turn to turn ("call_0"), so a whole-history map would pair a result with
    %% a later turn's function.
    {System, RevContents, _} = lists:foldl(fun split_one/2, {<<>>, [], #{}}, Msgs),
    SystemInstruction =
        case System of
            <<>> -> undefined;
            _ -> #{<<"parts">> => [#{<<"text">> => System}]}
        end,
    {SystemInstruction, lists:reverse(RevContents)}.

split_one(#{role := system} = M, {Sys, Contents, Names}) ->
    T = content_to_text(M),
    Sys1 =
        case Sys of
            <<>> -> T;
            _ -> <<Sys/binary, "\n\n", T/binary>>
        end,
    {Sys1, Contents, Names};
split_one(#{role := tool, tool_call_id := CallId, name := Name0} = M, {Sys, Contents, Names}) ->
    Name = thinwrap_llm_util:first_non_empty([
        Name0,
        case CallId of
            <<>> -> <<>>;
            _ -> maps:get(CallId, Names, <<>>)
        end,
        CallId,
        <<"tool">>
    ]),
    FR = with_vendor_id(CallId, #{
        <<"name">> => Name,
        <<"response">> => #{<<"result">> => parse_json_or_string(content_to_text(M))}
    }),
    Part = #{<<"functionResponse">> => FR},
    case Contents of
        [
            #{<<"role">> := <<"user">>, <<"parts">> := [#{<<"functionResponse">> := _} | _] = Ps} =
                Last
            | Rest
        ] ->
            {Sys, [Last#{<<"parts">> := Ps ++ [Part]} | Rest], Names};
        _ ->
            {Sys, [#{<<"role">> => <<"user">>, <<"parts">> => [Part]} | Contents], Names}
    end;
split_one(#{role := assistant, tool_calls := TCs} = M, {Sys, Contents, Names}) ->
    Names1 = lists:foldl(fun(#{id := Id, name := N}, Acc) -> Acc#{Id => N} end, Names, TCs),
    {Sys, [#{<<"role">> => <<"model">>, <<"parts">> => map_parts(M)} | Contents], Names1};
split_one(M, {Sys, Contents, Names}) ->
    {Sys, [#{<<"role">> => <<"user">>, <<"parts">> => map_parts(M)} | Contents], Names}.

content_to_text(#{content_parts := [_ | _] = Parts}) ->
    iolist_to_binary([T || #{type := text, text := T} <- Parts]);
content_to_text(#{content := C}) ->
    C.

map_parts(M) ->
    Parts0 =
        case M of
            #{content_parts := [_ | _] = CPs} ->
                [
                    case P of
                        #{type := text, text := T} ->
                            #{<<"text">> => T};
                        #{media_type := MT, base64 := B64} ->
                            #{<<"inlineData">> => #{<<"mimeType">> => MT, <<"data">> => B64}}
                    end
                 || P <- CPs
                ];
            #{content := C} when C =/= <<>> ->
                [#{<<"text">> => C}];
            _ ->
                []
        end,
    Parts =
        case M of
            #{role := assistant, tool_calls := [_ | _] = TCs} ->
                Parts0 ++ call_parts(TCs);
            _ ->
                Parts0
        end,
    case Parts of
        [] -> [#{<<"text">> => <<>>}];
        _ -> Parts
    end.

call_parts(TCs) ->
    Calls = [call_part(TC) || TC <- TCs],
    %% Gemini 3 400s a model turn whose function calls carry no signature. Only
    %% the first call of a turn is signed (parallel calls share it), so the
    %% documented bypass goes on the first call only, and only when none of the
    %% turn's calls has a real one.
    case lists:any(fun(P) -> maps:is_key(<<"thoughtSignature">>, P) end, Calls) of
        true ->
            Calls;
        false ->
            [First | Rest] = Calls,
            [First#{<<"thoughtSignature">> => ?SKIP_SIGNATURE} | Rest]
    end.

call_part(#{id := Id, name := Name, arguments := Args} = TC) ->
    Call = with_vendor_id(Id, #{<<"name">> => Name, <<"args">> => safe_json_parse(Args)}),
    Part = #{<<"functionCall">> => Call},
    case stored_signature(TC) of
        <<>> -> Part;
        Sig -> Part#{<<"thoughtSignature">> => Sig}
    end.

signature_metadata(Part) ->
    case thinwrap_llm_json:str(thinwrap_llm_json:get(Part, <<"thoughtSignature">>)) of
        <<>> -> undefined;
        Sig -> #{<<"gemini">> => #{<<"thoughtSignature">> => Sig}}
    end.

%% The signature Gemini issued for this call. Only the `gemini` key is read;
%% another provider's state in the same slot is never sent here.
stored_signature(#{provider_metadata := Meta}) ->
    thinwrap_llm_json:str(
        thinwrap_llm_json:get(thinwrap_llm_json:get(Meta, <<"gemini">>), <<"thoughtSignature">>)
    );
stored_signature(_) ->
    <<>>.

with_vendor_id(Id, Obj) ->
    case is_vendor_id(Id) of
        true -> Obj#{<<"id">> => Id};
        false -> Obj
    end.

%% Ids this connector makes up when Gemini sends none (`call_<n>`). They are
%% never echoed back to Gemini, which only knows the ids it issued.
is_vendor_id(<<>>) ->
    false;
is_vendor_id(Id) ->
    re:run(Id, "^call_\\d+$", [{capture, none}]) =/= match.

synthetic_id(I) -> <<"call_", (integer_to_binary(I))/binary>>.

function_calling_config(undefined) ->
    undefined;
function_calling_config(#{mode := auto}) ->
    #{<<"mode">> => <<"AUTO">>};
function_calling_config(#{mode := required}) ->
    #{<<"mode">> => <<"ANY">>};
function_calling_config(#{mode := none}) ->
    #{<<"mode">> => <<"NONE">>};
function_calling_config(#{mode := function, function_name := Name}) ->
    #{<<"mode">> => <<"ANY">>, <<"allowedFunctionNames">> => [Name]}.

apply_response_format(G, #{type := json_object}) ->
    G#{<<"responseMimeType">> => <<"application/json">>};
apply_response_format(G, #{type := json_schema} = RF) ->
    G1 = G#{<<"responseMimeType">> => <<"application/json">>},
    case RF of
        %% responseJsonSchema takes JSON Schema as written; responseSchema is
        %% the OpenAPI-subset form.
        #{json_schema := S} -> G1#{<<"responseJsonSchema">> => S};
        _ -> G1
    end;
apply_response_format(G, _) ->
    G.

parse_result(Raw, RequestedModel) ->
    Cand = first_candidate(Raw),
    Parts = candidate_parts(Cand),
    {Text, RevCalls, _} = lists:foldl(fun result_part/2, {<<>>, [], 0}, Parts),
    ToolCalls = lists:reverse(RevCalls),
    FinishReason =
        case ToolCalls of
            [_ | _] ->
                tool_calls;
            [] ->
                FR = thinwrap_llm_json:str(thinwrap_llm_json:get(Cand, <<"finishReason">>)),
                %% No candidate came back: a prompt-level safety block reports
                %% only promptFeedback.blockReason. Surface it as content_filter
                %% rather than a silent empty completion with an unknown finish.
                case FR =:= <<>> andalso block_reason(Raw) =/= <<>> of
                    true -> content_filter;
                    false -> map_finish_reason(FR)
                end
        end,
    #{
        message => #{role => assistant, content => Text, tool_calls => ToolCalls},
        finish_reason => FinishReason,
        usage => map_usage(thinwrap_llm_json:get(Raw, <<"usageMetadata">>)),
        model => thinwrap_llm_util:first_non_empty([
            thinwrap_llm_json:str(thinwrap_llm_json:get(Raw, <<"modelVersion">>)), RequestedModel
        ]),
        raw => Raw
    }.

result_part(P, {Text, Calls, Idx} = Acc) ->
    %% Reasoning/CoT parts (thought=true) are not normalized into content —
    %% they surface only via `raw` (mirrors Anthropic).
    case thinwrap_llm_json:truthy(thinwrap_llm_json:get(P, <<"thought">>)) of
        true ->
            Acc;
        false ->
            case thinwrap_llm_json:get(P, <<"text">>) of
                T when is_binary(T) ->
                    {<<Text/binary, T/binary>>, Calls, Idx};
                _ ->
                    case thinwrap_llm_json:get(P, <<"functionCall">>) of
                        undefined ->
                            Acc;
                        FC ->
                            TC0 = #{
                                id => thinwrap_llm_util:first_non_empty([
                                    thinwrap_llm_json:str(thinwrap_llm_json:get(FC, <<"id">>)),
                                    synthetic_id(Idx)
                                ]),
                                name => thinwrap_llm_json:str(
                                    thinwrap_llm_json:get(FC, <<"name">>)
                                ),
                                arguments => stringify_args(thinwrap_llm_json:get(FC, <<"args">>))
                            },
                            TC = thinwrap_llm_util:put_opt(
                                provider_metadata, signature_metadata(P), TC0
                            ),
                            {Text, [TC | Calls], Idx + 1}
                    end
            end
    end.

no_result(undefined) ->
    true;
no_result(Raw) ->
    thinwrap_llm_json:list(thinwrap_llm_json:get(Raw, <<"candidates">>)) =:= undefined andalso
        block_reason(Raw) =:= <<>>.

first_candidate(Raw) ->
    case thinwrap_llm_json:list(thinwrap_llm_json:get(Raw, <<"candidates">>)) of
        [C | _] -> C;
        _ -> undefined
    end.

candidate_parts(Cand) ->
    list_or_empty(thinwrap_llm_json:get(thinwrap_llm_json:get(Cand, <<"content">>), <<"parts">>)).

block_reason(Raw) ->
    thinwrap_llm_json:str(
        thinwrap_llm_json:get(thinwrap_llm_json:get(Raw, <<"promptFeedback">>), <<"blockReason">>)
    ).

list_or_empty(L) when is_list(L) -> L;
list_or_empty(_) -> [].

%% Values from the google-genai FinishReason enum. Everything else
%% (MALFORMED_FUNCTION_CALL, UNEXPECTED_TOOL_CALL, TOO_MANY_TOOL_CALLS,
%% LANGUAGE, OTHER, IMAGE_OTHER, NO_IMAGE, unspecified, ...) has no normalized
%% counterpart and stays `unknown`; the vendor value is on raw.
map_finish_reason(<<"STOP">>) -> stop;
map_finish_reason(<<"MAX_TOKENS">>) -> length;
%% Stopped at the per-request token limit before generation finished.
map_finish_reason(<<"CONTINUATION">>) -> length;
map_finish_reason(<<"SAFETY">>) -> content_filter;
map_finish_reason(<<"RECITATION">>) -> content_filter;
map_finish_reason(<<"BLOCKLIST">>) -> content_filter;
map_finish_reason(<<"PROHIBITED_CONTENT">>) -> content_filter;
map_finish_reason(<<"SPII">>) -> content_filter;
map_finish_reason(<<"IMAGE_SAFETY">>) -> content_filter;
map_finish_reason(<<"IMAGE_PROHIBITED_CONTENT">>) -> content_filter;
map_finish_reason(<<"IMAGE_RECITATION">>) -> content_filter;
map_finish_reason(_) -> unknown.

%% Tool-use prompt tokens are billed input and thinking tokens billed output,
%% but Gemini reports both apart from promptTokenCount / candidatesTokenCount.
%% Summing them keeps input + output equal to totalTokenCount.
map_usage(U) ->
    In = trunc(
        thinwrap_llm_json:num(thinwrap_llm_json:get(U, <<"promptTokenCount">>)) +
            thinwrap_llm_json:num(thinwrap_llm_json:get(U, <<"toolUsePromptTokenCount">>))
    ),
    Out = trunc(
        thinwrap_llm_json:num(thinwrap_llm_json:get(U, <<"candidatesTokenCount">>)) +
            thinwrap_llm_json:num(thinwrap_llm_json:get(U, <<"thoughtsTokenCount">>))
    ),
    #{input_tokens => In, output_tokens => Out, total_tokens => In + Out}.

%% An unrecognized effort falls back to medium: reasoning was asked for, and a
%% budget of 0 would switch thinking off.
effort_budget(<<"low">>) -> 1024;
effort_budget(<<"medium">>) -> 8192;
effort_budget(<<"high">>) -> 24576;
effort_budget(_) -> 8192.

stringify_args(undefined) ->
    <<"{}">>;
stringify_args(Args) ->
    try
        iolist_to_binary(json:encode(Args))
    catch
        error:_ -> <<"{}">>
    end.

safe_json_parse(S) ->
    try
        json:decode(S)
    catch
        error:_ -> #{}
    end.

parse_json_or_string(S) ->
    try
        json:decode(S)
    catch
        error:_ -> S
    end.

to_error(Status, Headers, Raw, Code0) ->
    Msg = thinwrap_llm_json:error_message(Raw),
    Code =
        case Code0 of
            undefined -> map_code(Status, Msg, Raw);
            _ -> Code0
        end,
    #{cause := #{} = Cause} = E = thinwrap_llm_error:vendor_error(Status, Headers, Raw, Code, Msg),
    %% Gemini has no rate-limit headers — retry timing lives in the 429 body's
    %% RetryInfo detail. Fill it in only when the Retry-After header didn't.
    case maps:is_key(<<"retryAfterSeconds">>, Cause) of
        true ->
            E;
        false ->
            case retry_delay_seconds(Raw) of
                undefined -> E;
                Secs -> E#{cause := Cause#{<<"retryAfterSeconds">> => Secs}}
            end
    end.

%% A Gemini error object delivered after HTTP 200 carries its own numeric
%% `code` (an HTTP status) plus a `status` string; classify from those rather
%% than the stream's 200 status.
stream_error_code(Body) ->
    Err = thinwrap_llm_json:get(Body, <<"error">>),
    case thinwrap_llm_json:int(thinwrap_llm_json:get(Err, <<"code">>)) of
        0 ->
            stream_status_code(
                Body,
                string:uppercase(thinwrap_llm_json:str(thinwrap_llm_json:get(Err, <<"status">>)))
            );
        Code ->
            map_code(Code, thinwrap_llm_json:error_message(Body), Body)
    end.

stream_status_code(Body, Status) ->
    case is_api_key_invalid(Body) of
        true ->
            auth_failed;
        false ->
            case Status of
                <<"RESOURCE_EXHAUSTED">> ->
                    rate_limited;
                S when S =:= <<"UNAUTHENTICATED">>; S =:= <<"PERMISSION_DENIED">> ->
                    auth_failed;
                S when
                    S =:= <<"INVALID_ARGUMENT">>;
                    S =:= <<"FAILED_PRECONDITION">>;
                    S =:= <<"NOT_FOUND">>;
                    S =:= <<"OUT_OF_RANGE">>;
                    S =:= <<"ALREADY_EXISTS">>
                ->
                    case is_context_overflow(thinwrap_llm_json:error_message(Body)) of
                        true -> context_length_exceeded;
                        false -> invalid_request
                    end;
                S when
                    S =:= <<"UNAVAILABLE">>;
                    S =:= <<"INTERNAL">>;
                    S =:= <<"DEADLINE_EXCEEDED">>;
                    S =:= <<"ABORTED">>
                ->
                    provider_unavailable;
                %% A frame with neither a code nor a known status says nothing
                %% classifiable.
                _ ->
                    unknown
            end
    end.

map_code(S, _Msg, _Body) when S =:= 401; S =:= 403 ->
    auth_failed;
%% 402: the prepay credit balance is depleted, which Gemini itself labels
%% RESOURCE_EXHAUSTED like a 429; classified as rate-limited, as OpenAI's 429
%% insufficient_quota is.
map_code(S, _Msg, _Body) when S =:= 402; S =:= 429 ->
    rate_limited;
%% 408 is a transient timeout; Google's troubleshooting guide says to retry.
map_code(S, _Msg, _Body) when S >= 500; S =:= 408 ->
    provider_unavailable;
map_code(S, Msg, Body) when S >= 400 ->
    %% A bad API key is a 400 INVALID_ARGUMENT; only the ErrorInfo reason tells
    %% it apart from a malformed request.
    case is_api_key_invalid(Body) of
        true ->
            auth_failed;
        false ->
            case is_context_overflow(Msg) of
                true -> context_length_exceeded;
                %% 404, 413, 422, …: the request cannot succeed as sent.
                false -> invalid_request
            end
    end;
map_code(_, _, _) ->
    unknown.

is_api_key_invalid(Body) ->
    lists:any(
        fun(D) ->
            thinwrap_llm_json:str(thinwrap_llm_json:get(D, <<"reason">>)) =:= <<"API_KEY_INVALID">>
        end,
        list_or_empty(
            thinwrap_llm_json:get(thinwrap_llm_json:get(Body, <<"error">>), <<"details">>)
        )
    ).

%% Gemini reports an oversized prompt as a 400 INVALID_ARGUMENT ("The input
%% token count (N) exceeds the maximum number of tokens allowed (M)"), so only
%% that wording tells it apart. Looser matches also hit ordinary validation
%% errors, and a caller that trims history on this code would loop forever.
is_context_overflow(Msg) ->
    binary:match(string:lowercase(Msg), <<"exceeds the maximum number of tokens">>) =/= nomatch.

retry_delay_seconds(Body) ->
    case
        thinwrap_llm_json:list(
            thinwrap_llm_json:get(thinwrap_llm_json:get(Body, <<"error">>), <<"details">>)
        )
    of
        undefined -> undefined;
        Details -> first_delay(Details)
    end.

first_delay([]) ->
    undefined;
first_delay([D | Rest]) ->
    case thinwrap_llm_json:str(thinwrap_llm_json:get(D, <<"retryDelay">>)) of
        <<>> ->
            first_delay(Rest);
        RD ->
            case re:run(RD, "^(\\d+(?:\\.\\d+)?)s$", [{capture, all_but_first, binary}]) of
                {match, [N]} -> floor(to_number(N) + 0.5);
                nomatch -> first_delay(Rest)
            end
    end.

to_number(N) ->
    case binary:match(N, <<".">>) of
        nomatch -> float(binary_to_integer(N));
        _ -> binary_to_float(N)
    end.
