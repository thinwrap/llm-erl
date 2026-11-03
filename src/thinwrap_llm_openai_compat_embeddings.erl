-module(thinwrap_llm_openai_compat_embeddings).
-moduledoc false.

%% The shared embeddings connector for the OpenAI-float-shaped providers
%% (`POST <base_url>/embeddings` with `encoding_format: "float"`).

-export([embed/2]).

-doc false.
-spec embed(thinwrap_llm:config(), thinwrap_llm:embeddings_input()) ->
    thinwrap_llm:embeddings_result().
embed(Config, #{model := Model, input := Inputs} = Input) ->
    Spec = thinwrap_llm_spec:get(maps:get(provider, Config)),
    B0 = #{<<"model">> => Model, <<"input">> => Inputs, <<"encoding_format">> => <<"float">>},
    B1 =
        case Input of
            #{dimensions := D} -> B0#{(maps:get(dimensions_field, Spec)) => D};
            _ -> B0
        end,
    Body = thinwrap_llm_passthrough:apply_body(B1, Input),
    Headers = thinwrap_llm_passthrough:merge_headers(
        thinwrap_llm_openai_compat:auth_headers(Spec, Config), Input
    ),
    %% passthrough.query is honoured here too (e.g. Azure OpenAI's required
    %% api-version), as on the chat path.
    Url = thinwrap_llm_passthrough:append_query(
        <<(thinwrap_llm_openai_compat:base_url(Spec, Config))/binary, "/embeddings">>, Input
    ),
    #{status := Status, headers := RespHeaders, body := RespBody} =
        thinwrap_llm_http:post_json(Config, Url, Body, Headers),
    Raw = thinwrap_llm_json:decode(RespBody),
    thinwrap_llm_util:ok_status(Status) orelse
        thinwrap_llm_error:fail(
            thinwrap_llm_error:vendor_error(
                Status, RespHeaders, Raw, map_code(Status), thinwrap_llm_json:error_message(Raw)
            )
        ),
    Raw =:= undefined andalso thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status)),
    case thinwrap_llm_json:get(Raw, <<"error">>) of
        undefined ->
            ok;
        Err ->
            %% An error object on a 2xx (OpenRouter-style) keeps the 2xx it
            %% arrived with as its status; its `error.code` (a number, or a
            %% name such as insufficient_quota) classifies it the way chat
            %% does, and without one it says nothing classifiable.
            Code =
                case thinwrap_llm_error:embedded_error_class(Err) of
                    undefined -> unknown;
                    Effective -> map_code(Effective)
                end,
            thinwrap_llm_error:fail(
                thinwrap_llm_error:vendor_error(
                    Status,
                    RespHeaders,
                    Raw,
                    Code,
                    thinwrap_llm_json:error_message(#{<<"error">> => Err})
                )
            )
    end,
    Data =
        case thinwrap_llm_json:list(thinwrap_llm_json:get(Raw, <<"data">>)) of
            undefined -> thinwrap_llm_error:fail(thinwrap_llm_error:empty_body(Status));
            Rows0 -> Rows0
        end,
    %% The result promises one vector per input in input order; a short or
    %% padded `data` cannot be re-aligned to the inputs.
    length(Data) =:= length(Inputs) orelse
        thinwrap_llm_error:fail(
            thinwrap_llm_error:malformed_body(
                Status,
                Raw,
                iolist_to_binary([
                    "provider returned ",
                    integer_to_binary(length(Data)),
                    " embeddings for ",
                    integer_to_binary(length(Inputs)),
                    " inputs"
                ])
            )
        ),
    Rows = [
        Row
     || {_, Row} <- lists:keysort(1, [
            {thinwrap_llm_json:num(thinwrap_llm_json:get(R, <<"index">>)), R}
         || R <- Data
        ])
    ],
    Embeddings = [vector(R, Status, Raw) || R <- Rows],
    Usage = thinwrap_llm_json:get(Raw, <<"usage">>),
    InTok = thinwrap_llm_json:int(thinwrap_llm_json:get(Usage, <<"prompt_tokens">>)),
    Total =
        case thinwrap_llm_json:int(thinwrap_llm_json:get(Usage, <<"total_tokens">>)) of
            0 -> InTok;
            T -> T
        end,
    #{
        embeddings => Embeddings,
        usage => #{input_tokens => InTok, total_tokens => Total},
        model => thinwrap_llm_util:first_non_empty([
            thinwrap_llm_json:str(thinwrap_llm_json:get(Raw, <<"model">>)), Model
        ]),
        raw => Raw
    }.

%% A null/string cell is not a coordinate; coercing it to 0.0 would return a
%% plausible-looking but wrong vector.
vector(Row, Status, Raw) ->
    case thinwrap_llm_json:list(thinwrap_llm_json:get(Row, <<"embedding">>)) of
        undefined ->
            thinwrap_llm_error:fail(
                thinwrap_llm_error:malformed_body(
                    Status, Raw, <<"provider returned an embedding row without a vector">>
                )
            );
        Cells ->
            [
                case is_number(C) of
                    true ->
                        float(C);
                    false ->
                        thinwrap_llm_error:fail(
                            thinwrap_llm_error:malformed_body(
                                Status, Raw, <<"provider returned a non-numeric embedding value">>
                            )
                        )
                end
             || C <- Cells
            ]
    end.

map_code(S) when S =:= 401; S =:= 403 -> auth_failed;
%% 402 is out of credit/quota, which OpenAI reports as 429 insufficient_quota.
map_code(S) when S =:= 402; S =:= 429 -> rate_limited;
%% 408 is a timeout on the provider's side; a retry can succeed.
map_code(S) when S >= 500; S =:= 408 -> provider_unavailable;
%% 400, 404, 413, 422, …: the request cannot succeed as sent.
map_code(S) when S >= 400 -> invalid_request;
map_code(_) -> unknown.
