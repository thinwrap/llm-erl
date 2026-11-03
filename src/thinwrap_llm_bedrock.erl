-module(thinwrap_llm_bedrock).
-moduledoc false.

%% Native adapter for AWS Bedrock's unified Converse API
%% (`POST <origin>/model/{modelId}/converse`).
%%
%% Requests are signed with the hand-rolled SigV4 signer
%% (`thinwrap_llm_sigv4`) — zero third-party deps. Converse is structurally
%% different from OpenAI Chat Completions (top-level `system[]`, typed content
%% blocks, `inferenceConfig`, `toolConfig`, tool results carried in a user
%% turn), so this is a full translation layer emitting the identical
%% normalized result / stream deltas / connector error.
%%
%% Streaming: Bedrock's `converse-stream` uses AWS's binary event-stream
%% framing (`application/vnd.amazon.eventstream`), not SSE. As in the sibling
%% libraries, this adapter does NOT parse that protocol — `stream/4` makes one
%% non-incremental Converse call and folds the whole result as deltas (a
%% content delta, one delta per tool call, then a terminal finish/usage/raw
%% delta).
%%
%% Not normalized here (passthrough/raw, never emulated): reasoning control
%% (model-dependent on Bedrock — pass `additionalModelRequestFields` via
%% `passthrough.body`), response_format, prompt caching, and top_k.

-export([complete/2, stream/4]).

%% Converse requires inferenceConfig.maxTokens; used when neither the input's
%% max_output_tokens nor the config's default_max_tokens is set.
-define(DEFAULT_MAX_TOKENS, 4096).
%% AWS service name folded into the SigV4 credential scope.
-define(SERVICE, <<"bedrock">>).
%% An AWS region is lowercase DNS-label text (us-east-1, us-gov-west-1). It is
%% interpolated into the default host, so anything else could point the signed
%% request, session token included, at a host of the caller's input's choosing.
-define(REGION_RE, "\\A[a-z0-9]+(?:-[a-z0-9]+)*\\z").

-doc false.
-spec complete(thinwrap_llm:config(), thinwrap_llm:chat_input()) -> thinwrap_llm:chat_result().
complete(Config, Input) ->
    Endpoint = endpoint(Config),
    {Url, BodyBytes, Headers} = build_signed_request(Endpoint, Config, Input),
    %% Send the EXACT signed bytes so the request the transport writes matches
    %% the SigV4 payload hash + canonical path.
    #{status := Status, headers := RespHeaders, body := RespBody} =
        thinwrap_llm_http:post_raw(Config, Url, <<"application/json">>, BodyBytes, Headers),
    Raw = thinwrap_llm_json:decode(RespBody),
    thinwrap_llm_util:ok_status(Status) orelse
        thinwrap_llm_error:fail(to_error(Status, RespHeaders, Raw)),
    %% A Converse response always carries an `output` object; without it this
    %% is not a result, and parsing it would fake an empty completion.
    case thinwrap_llm_json:get(Raw, <<"output">>) of
        #{} -> ok;
        _ -> thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status))
    end,
    parse_result(Raw, maps:get(model, Input)).

-doc false.
-spec stream(
    thinwrap_llm:config(),
    thinwrap_llm:chat_input(),
    fun((thinwrap_llm:stream_delta(), Acc) -> Acc),
    Acc
) -> Acc.
stream(Config, Input, Fun, Acc0) ->
    %% Non-incremental fallback (see the module comment): one Converse call,
    %% then the full result as deltas.
    #{message := #{content := Content, tool_calls := ToolCalls}} = R = complete(Config, Input),
    ContentDeltas =
        case Content of
            <<>> -> [];
            _ -> [#{content_delta => Content}]
        end,
    ToolDeltas = [
        #{tool_call_delta => tool_call_delta(I, TC)}
     || {I, TC} <- lists:enumerate(0, ToolCalls)
    ],
    Terminal = #{
        finish_reason => maps:get(finish_reason, R),
        usage => maps:get(usage, R),
        raw => maps:get(raw, R)
    },
    lists:foldl(Fun, Acc0, ContentDeltas ++ ToolDeltas ++ [Terminal]).

tool_call_delta(I, #{id := Id, name := Name, arguments := Args}) ->
    thinwrap_llm_util:put_opt(
        arguments_delta,
        Args,
        thinwrap_llm_util:put_opt(
            function_name, Name, thinwrap_llm_util:put_opt(id, Id, #{index => I})
        )
    ).

%% ---------------------------------------------------------------------------
%% endpoint

endpoint(Config) ->
    Region = maps:get(region, Config),
    match(Region, ?REGION_RE) orelse
        thinwrap_llm_error:fail(
            thinwrap_llm_error:invalid_request(
                <<"Bedrock: `region` must be an AWS region name such as 'us-east-1'">>
            )
        ),
    Origin0 =
        case maps:get(base_url, Config, <<>>) of
            <<>> -> <<"https://bedrock-runtime.", Region/binary, ".amazonaws.com">>;
            B -> B
        end,
    Origin = rstrip_slash(Origin0),
    {Scheme, Netloc, Path} = split_url(Origin),
    Netloc =:= <<>> andalso
        thinwrap_llm_error:fail(
            thinwrap_llm_error:invalid_request(
                <<
                    "Bedrock: invalid `base_url`/`region` \x{2014} "
                    "could not derive a host from '"/utf8,
                    Origin/binary,
                    "'"
                >>
            )
        ),
    %% A client omits a default port from the Host header it sends, so the
    %% signed host must omit it too or the signature never matches.
    Host =
        case default_port(string:lowercase(Scheme)) of
            undefined -> Netloc;
            Port -> strip_suffix(Netloc, Port)
        end,
    %% A base_url may carry a path prefix (a gateway mount point). The wire URL
    %% already includes it via the origin; the signed canonical URI must
    %% include it too, each segment encoded once more as for the model id.
    Segments = [S || S <- binary:split(Path, <<"/">>, [global]), S =/= <<>>],
    BasePath = iolist_to_binary([[<<"/">>, thinwrap_llm_util:rfc3986(S)] || S <- Segments]),
    #{origin => Origin, host => Host, base_path => BasePath, region => Region}.

match(Bin, Re) -> re:run(Bin, Re, [{capture, none}]) =:= match.

default_port(<<"https">>) -> <<":443">>;
default_port(<<"http">>) -> <<":80">>;
default_port(_) -> undefined.

strip_suffix(Bin, Suffix) ->
    N = byte_size(Bin) - byte_size(Suffix),
    case N >= 0 andalso binary:part(Bin, N, byte_size(Suffix)) =:= Suffix of
        true -> binary:part(Bin, 0, N);
        false -> Bin
    end.

rstrip_slash(<<>>) ->
    <<>>;
rstrip_slash(B) ->
    case binary:last(B) of
        $/ -> rstrip_slash(binary:part(B, 0, byte_size(B) - 1));
        _ -> B
    end.

%% {Scheme, Netloc, Path} the way a urlsplit reads them: the raw authority
%% (userinfo and port included) only after `scheme://` or `//`.
split_url(Url) ->
    {Scheme, Rest} =
        case
            re:run(Url, "\\A([A-Za-z][A-Za-z0-9+.-]*):(.*)\\z", [
                {capture, all_but_first, binary}, dotall
            ])
        of
            {match, [S, R]} -> {S, R};
            nomatch -> {<<>>, Url}
        end,
    case Rest of
        <<"//", AfterSlashes/binary>> ->
            {Netloc, Tail} =
                case binary:match(AfterSlashes, [<<"/">>, <<"?">>, <<"#">>]) of
                    {P, _} ->
                        {
                            binary:part(AfterSlashes, 0, P),
                            binary:part(AfterSlashes, P, byte_size(AfterSlashes) - P)
                        };
                    nomatch ->
                        {AfterSlashes, <<>>}
                end,
            {Scheme, Netloc, path_only(Tail)};
        _ ->
            {Scheme, <<>>, path_only(Rest)}
    end.

path_only(Tail) ->
    case binary:match(Tail, [<<"?">>, <<"#">>]) of
        {P, _} -> binary:part(Tail, 0, P);
        nomatch -> Tail
    end.

%% ---------------------------------------------------------------------------
%% request build

%% Serialize the Converse body ONCE, then SigV4-sign those exact bytes together
%% with the canonical path so URL/body/signature all agree.
build_signed_request(
    #{origin := Origin, host := Host, base_path := BasePath, region := Region}, Config, Input
) ->
    BodyBytes =
        try
            thinwrap_llm_json:encode(build_request(Config, Input))
        catch
            throw:{thinwrap_llm_error, _} ->
                thinwrap_llm_error:fail(
                    thinwrap_llm_error:invalid_request(<<"bedrock: failed to encode request body">>)
                )
        end,
    %% The WIRE path is single-encoded; the SIGNED canonical path is
    %% DOUBLE-encoded. For every non-S3 AWS service the server URI-encodes the
    %% received path a second time before recomputing the signature, so the
    %% client's canonical path must be double-encoded relative to the raw model
    %% id. Model ids with ':' (on-demand IDs `…-v2:0`, cross-region profiles
    %% `us.anthropic.…`, ARNs with ':' / '/') otherwise sign %3A while the
    %% server computes %253A → 403 SignatureDoesNotMatch. A colon-free id
    %% double-encodes to itself, so this is a no-op for those.
    Encoded = thinwrap_llm_util:rfc3986(maps:get(model, Input)),
    WirePath = <<"/model/", Encoded/binary, "/converse">>,
    CanonicalPath =
        <<BasePath/binary, "/model/", (thinwrap_llm_util:rfc3986(Encoded))/binary, "/converse">>,
    %% passthrough.query must be folded into BOTH the SigV4 canonical query
    %% string and the wire URL — identical bytes (RFC 3986-encoded, key-sorted)
    %% — so the signature the server recomputes from the received query still
    %% matches. Appending it unsigned would break the signature.
    CanonicalQuery =
        case thinwrap_llm_passthrough:query(Input) of
            Q when map_size(Q) =:= 0 -> <<>>;
            Q -> thinwrap_llm_sigv4:canonical_query(Q)
        end,
    Url =
        case CanonicalQuery of
            <<>> -> <<Origin/binary, WirePath/binary>>;
            _ -> <<Origin/binary, WirePath/binary, "?", CanonicalQuery/binary>>
        end,
    %% Headers folded into the signature: Content-Type + config headers +
    %% per-request passthrough headers. Merged case-insensitively: the signer
    %% lowercases names, so "content-type" beside "Content-Type" would sign one
    %% value while a transport may send both (→ SignatureDoesNotMatch).
    PtHeaders = maps:get(headers, maps:get(passthrough, Input, #{}), #{}),
    Extra0 = lists:foldl(
        fun({K, V}, Acc) -> thinwrap_llm_util:set_header_ci(Acc, K, V) end,
        #{<<"Content-Type">> => <<"application/json">>},
        maps:to_list(maps:get(headers, Config, #{})) ++ maps:to_list(PtHeaders)
    ),
    %% post_raw force-SENDS application/json; SIGN that same value so a config
    %% or passthrough Content-Type override can't desync the signature from
    %% what actually goes on the wire (→ SignatureDoesNotMatch).
    Extra = thinwrap_llm_util:set_header_ci(Extra0, <<"Content-Type">>, <<"application/json">>),
    Signed = thinwrap_llm_sigv4:sign(#{
        method => <<"POST">>,
        service => ?SERVICE,
        region => Region,
        host => Host,
        path => CanonicalPath,
        canonical_query => CanonicalQuery,
        payload => BodyBytes,
        access_key_id => thinwrap_llm_input:secret(Config, access_key_id),
        secret_access_key => thinwrap_llm_input:secret(Config, secret_access_key),
        session_token => thinwrap_llm_input:secret(Config, session_token),
        signed => Extra
    }),
    Headers = maps:fold(
        fun
            %% Host is derived by the transport from the URL; attaching it as
            %% a request header is a no-op, so it is dropped.
            (<<"Host">>, _V, Acc) -> Acc;
            (K, V, Acc) -> thinwrap_llm_util:set_header_ci(Acc, K, V)
        end,
        Extra,
        Signed
    ),
    {Url, BodyBytes, Headers}.

build_request(Config, #{messages := Msgs, tools := Tools, stop := Stop} = Input) ->
    {System, Messages} = split_messages(Msgs),
    B0 = #{<<"messages">> => Messages},
    B1 =
        case System of
            [] -> B0;
            _ -> B0#{<<"system">> => System}
        end,
    MaxTokens =
        case {Input, maps:get(default_max_tokens, Config, 0)} of
            {#{max_output_tokens := M}, _} -> M;
            {_, 0} -> ?DEFAULT_MAX_TOKENS;
            {_, D} -> D
        end,
    IC0 = #{<<"maxTokens">> => MaxTokens},
    IC1 = opt(<<"temperature">>, maps:get(temperature, Input, undefined), IC0),
    IC2 = opt(<<"topP">>, maps:get(top_p, Input, undefined), IC1),
    IC3 =
        case Stop of
            [] -> IC2;
            _ -> IC2#{<<"stopSequences">> => Stop}
        end,
    B2 = B1#{<<"inferenceConfig">> => IC3},
    %% Converse has no "none" tool choice; withholding the tools is the only way
    %% to keep the model from calling one.
    B3 =
        case {Tools, Input} of
            {[], _} ->
                B2;
            {_, #{tool_choice := #{mode := none}}} ->
                B2;
            _ ->
                TC0 = #{<<"tools">> => [tool_spec(T) || T <- Tools]},
                TC1 = opt(
                    <<"toolChoice">>, map_tool_choice(maps:get(tool_choice, Input, undefined)), TC0
                ),
                B2#{<<"toolConfig">> => TC1}
        end,
    %% Reasoning control is model-dependent on Bedrock, so it is not mapped;
    %% pass additionalModelRequestFields via passthrough.body.
    thinwrap_llm_passthrough:apply_body(B3, Input).

opt(_K, undefined, M) -> M;
opt(K, V, M) -> M#{K => V}.

tool_spec(#{name := Name} = T) ->
    S0 = #{<<"name">> => Name},
    S1 =
        case T of
            #{description := D} when D =/= <<>> -> S0#{<<"description">> => D};
            _ -> S0
        end,
    Schema = maps:get(parameters, T, #{<<"type">> => <<"object">>, <<"properties">> => #{}}),
    #{<<"toolSpec">> => S1#{<<"inputSchema">> => #{<<"json">> => Schema}}}.

map_tool_choice(#{mode := auto}) -> #{<<"auto">> => #{}};
map_tool_choice(#{mode := required}) -> #{<<"any">> => #{}};
map_tool_choice(#{mode := function, function_name := N}) -> #{<<"tool">> => #{<<"name">> => N}};
map_tool_choice(_) -> undefined.

%% Hoist system turns into a top-level system[] and map the rest into Converse
%% {role, content[]} turns; a tool result folds into the preceding user turn
%% (Converse carries tool results in a user message).
split_messages(Messages) ->
    {Sys, Out} = lists:foldl(fun split_one/2, {[], []}, Messages),
    {lists:reverse(Sys), lists:reverse(Out)}.

split_one(#{role := system} = M, {Sys, Out}) ->
    case content_to_text(M) of
        <<>> -> {Sys, Out};
        T -> {[#{<<"text">> => T} | Sys], Out}
    end;
split_one(#{role := tool} = M, {Sys, Out}) ->
    Block = #{
        <<"toolResult">> => #{
            <<"toolUseId">> => maps:get(tool_call_id, M),
            <<"content">> => [#{<<"text">> => content_to_text(M)}]
        }
    },
    case Out of
        [#{<<"role">> := <<"user">>, <<"content">> := Content} = Last | Rest] ->
            case is_tool_result_content(Content) of
                true -> {Sys, [Last#{<<"content">> := Content ++ [Block]} | Rest]};
                false -> {Sys, [#{<<"role">> => <<"user">>, <<"content">> => [Block]} | Out]}
            end;
        _ ->
            {Sys, [#{<<"role">> => <<"user">>, <<"content">> => [Block]} | Out]}
    end;
split_one(#{role := Role} = M, {Sys, Out}) ->
    {Sys, [#{<<"role">> => atom_to_binary(Role), <<"content">> => map_content_blocks(M)} | Out]}.

content_to_text(#{content_parts := [_ | _] = Parts}) ->
    iolist_to_binary([maps:get(text, P) || #{type := text} = P <- Parts]);
content_to_text(#{content := C}) ->
    C.

map_content_blocks(M) ->
    Blocks0 =
        case M of
            #{content_parts := [_ | _] = Parts} ->
                lists:flatmap(
                    fun
                        %% Converse rejects a blank text block, so an empty part
                        %% is dropped.
                        (#{type := text, text := <<>>}) ->
                            [];
                        (#{type := text, text := T}) ->
                            [#{<<"text">> => T}];
                        (#{media_type := MT, base64 := B64}) ->
                            [
                                #{
                                    <<"image">> => #{
                                        <<"format">> => image_format(MT),
                                        <<"source">> => #{<<"bytes">> => B64}
                                    }
                                }
                            ]
                    end,
                    Parts
                );
            #{content := C} when C =/= <<>> ->
                [#{<<"text">> => C}];
            _ ->
                []
        end,
    Blocks1 =
        case M of
            #{role := assistant, tool_calls := [_ | _] = TCs} ->
                Blocks0 ++
                    [
                        #{
                            <<"toolUse">> => #{
                                <<"toolUseId">> => Id,
                                <<"name">> => Name,
                                <<"input">> => safe_json_parse(Args)
                            }
                        }
                     || #{id := Id, name := Name, arguments := Args} <- TCs
                    ];
            _ ->
                Blocks0
        end,
    case Blocks1 of
        [] -> [#{<<"text">> => <<>>}];
        _ -> Blocks1
    end.

is_tool_result_content([#{<<"toolResult">> := _} | _]) -> true;
is_tool_result_content(_) -> false.

image_format(MediaType) ->
    M = string:lowercase(MediaType),
    Has = fun(N) -> binary:match(M, N) =/= nomatch end,
    case {Has(<<"png">>), Has(<<"jpeg">>) orelse Has(<<"jpg">>), Has(<<"gif">>), Has(<<"webp">>)} of
        {true, _, _, _} -> <<"png">>;
        {_, true, _, _} -> <<"jpeg">>;
        {_, _, true, _} -> <<"gif">>;
        {_, _, _, true} -> <<"webp">>;
        _ -> <<"png">>
    end.

safe_json_parse(S) ->
    case thinwrap_llm_json:decode(S) of
        undefined -> #{};
        null -> #{};
        V -> V
    end.

%% ---------------------------------------------------------------------------
%% response parse

parse_result(Raw, RequestedModel) ->
    Blocks = list_or_empty(
        thinwrap_llm_json:get(
            thinwrap_llm_json:get(thinwrap_llm_json:get(Raw, <<"output">>), <<"message">>),
            <<"content">>
        )
    ),
    {Texts, ToolCalls} = lists:foldl(fun parse_block/2, {[], []}, Blocks),
    Usage = thinwrap_llm_json:get(Raw, <<"usage">>),
    %% Cache reads and writes are billed input but reported apart from
    %% inputTokens; the normalized count includes them.
    In = trunc(
        thinwrap_llm_json:num(thinwrap_llm_json:get(Usage, <<"inputTokens">>)) +
            thinwrap_llm_json:num(thinwrap_llm_json:get(Usage, <<"cacheReadInputTokens">>)) +
            thinwrap_llm_json:num(thinwrap_llm_json:get(Usage, <<"cacheWriteInputTokens">>))
    ),
    Out = thinwrap_llm_json:int(thinwrap_llm_json:get(Usage, <<"outputTokens">>)),
    #{
        message => #{
            role => assistant,
            content => iolist_to_binary(lists:reverse(Texts)),
            tool_calls => lists:reverse(ToolCalls)
        },
        finish_reason => map_stop_reason(
            thinwrap_llm_json:str(thinwrap_llm_json:get(Raw, <<"stopReason">>))
        ),
        %% Derived rather than taken from totalTokens, so it always equals
        %% input + output under the cache-inclusive input above.
        usage => #{input_tokens => In, output_tokens => Out, total_tokens => In + Out},
        model => RequestedModel,
        raw => Raw
    }.

parse_block(B, {Texts, TCs}) ->
    case thinwrap_llm_json:get(B, <<"text">>) of
        T when is_binary(T) ->
            {[T | Texts], TCs};
        _ ->
            case thinwrap_llm_json:get(B, <<"toolUse">>) of
                undefined ->
                    {Texts, TCs};
                TU ->
                    Args =
                        case thinwrap_llm_json:get(TU, <<"input">>) of
                            undefined -> <<"{}">>;
                            Inp -> thinwrap_llm_json:encode(Inp)
                        end,
                    TC = #{
                        id => thinwrap_llm_json:str(thinwrap_llm_json:get(TU, <<"toolUseId">>)),
                        name => thinwrap_llm_json:str(thinwrap_llm_json:get(TU, <<"name">>)),
                        arguments => Args
                    },
                    {Texts, [TC | TCs]}
            end
    end.

list_or_empty(L) when is_list(L) -> L;
list_or_empty(_) -> [].

map_stop_reason(<<"end_turn">>) -> stop;
map_stop_reason(<<"stop_sequence">>) -> stop;
map_stop_reason(<<"max_tokens">>) -> length;
map_stop_reason(<<"tool_use">>) -> tool_calls;
map_stop_reason(<<"content_filtered">>) -> content_filter;
map_stop_reason(<<"guardrail_intervened">>) -> content_filter;
%% The output was cut off by the context window rather than maxTokens.
map_stop_reason(<<"model_context_window_exceeded">>) -> length;
map_stop_reason(_) -> unknown.

%% ---------------------------------------------------------------------------
%% errors

to_error(Status, Headers, Raw) ->
    Msg =
        case extract_error(Raw) of
            <<>> -> <<"HTTP ", (integer_to_binary(Status))/binary>>;
            M -> M
        end,
    thinwrap_llm_error:vendor_error(
        Status, Headers, Raw, map_code(Status, Msg, error_type(Headers, Raw)), Msg
    ).

extract_error(B) when is_binary(B) ->
    B;
extract_error(#{} = Body) ->
    first_message([
        maps:get(K, Body, undefined)
     || K <- [<<"message">>, <<"Message">>, <<"__type">>]
    ]);
extract_error(_) ->
    <<>>.

first_message([]) -> <<>>;
first_message([V | _]) when is_binary(V), V =/= <<>> -> V;
first_message([_ | Rest]) -> first_message(Rest).

%% The AWS error shape name (`ThrottlingException`). restJson services put it
%% in the `x-amzn-ErrorType` header, sometimes followed by `:<uri>`; a body
%% `__type` may carry a `namespace#` prefix.
error_type(Headers, Body) ->
    Raw =
        case maps:get(<<"x-amzn-errortype">>, Headers, <<>>) of
            <<>> when is_map(Body) ->
                thinwrap_llm_json:str(maps:get(<<"__type">>, Body, undefined));
            <<>> ->
                <<>>;
            H ->
                H
        end,
    [BeforeColon | _] = binary:split(Raw, <<":">>),
    AfterHash = lists:last(binary:split(BeforeColon, <<"#">>, [global])),
    string:trim(AfterHash).

map_code(S, _Msg, _Type) when S =:= 401; S =:= 403 ->
    auth_failed;
map_code(S, _Msg, Type) when S =:= 429; Type =:= <<"ThrottlingException">> ->
    rate_limited;
%% 408 ModelTimeoutException and 424 ModelErrorException are failures of the
%% model behind the request, not of the request itself.
map_code(S, _Msg, _Type) when S >= 500; S =:= 408; S =:= 424 ->
    provider_unavailable;
map_code(S, Msg0, _Type) when S =:= 400; S =:= 404; S =:= 422 ->
    Msg = string:lowercase(Msg0),
    Has = fun(N) -> binary:match(Msg, N) =/= nomatch end,
    case Has(<<"throttl">>) of
        true ->
            rate_limited;
        false ->
            %% Tight phrases only: Bedrock reports an oversized prompt as an
            %% ordinary ValidationException and relays the model provider's own
            %% wording, and a looser match ("token", "max", "too long") also
            %% hits validation errors a caller trimming history cannot fix.
            case
                lists:any(Has, [
                    <<"input is too long">>,
                    <<"prompt is too long">>,
                    <<"too many input tokens">>,
                    <<"maximum context length">>,
                    <<"exceed context limit">>
                ])
            of
                true -> context_length_exceeded;
                false -> invalid_request
            end
    end;
%% 402 means the account is out of credit or quota. OpenAI (429
%% insufficient_quota) and Gemini (RESOURCE_EXHAUSTED) already report that as
%% rate-limited, so 402 classifies the same way.
map_code(402, _Msg, _Type) ->
    rate_limited;
map_code(413, _Msg, _Type) ->
    invalid_request;
map_code(_, _Msg, _Type) ->
    unknown.
