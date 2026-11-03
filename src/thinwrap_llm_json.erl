-module(thinwrap_llm_json).
-moduledoc false.

%% JSON on OTP's own `json` module (OTP 27+), plus absence-tolerant accessors
%% for walking a decoded vendor body, where full typing would be overkill.
%%
%% Decoded shape: objects are maps with binary keys, arrays are lists, `null`
%% is the atom `null`, numbers stay integer or float as written. `get/2` folds
%% "absent" and JSON `null` into `undefined`, as a None-returning lookup does.

-export([
    encode/1,
    decode/1,
    get/2,
    str/1,
    num/1,
    int/1,
    list/1,
    truthy/1,
    error_message/1
]).

-doc false.
%% Compact UTF-8 JSON. Atom keys/values other than true/false/null encode as
%% strings. A non-UTF-8 binary or an unencodable term is request data the
%% vendor could never accept, so it fails with `invalid_request`.
-spec encode(term()) -> binary().
encode(Term) ->
    try
        %% encode/2 with the default encoder is json:encode/1 for any term
        %% (encode/1's spec is narrower than what it accepts and rejects).
        iolist_to_binary(json:encode(Term, fun json:encode_value/2))
    catch
        error:Reason ->
            thinwrap_llm_error:fail(
                thinwrap_llm_error:new(invalid_request, #{
                    message => <<"Failed to encode request body">>,
                    provider_message => <<"Failed to encode request body">>,
                    cause => encode_reason(Reason)
                })
            )
    end.

%% Keep the offending value out of the error: it could be a credential.
encode_reason({invalid_byte, _}) -> invalid_byte;
encode_reason({unsupported_type, _}) -> unsupported_type;
encode_reason(R) when is_atom(R) -> R;
encode_reason(_) -> encode_failed.

-doc false.
%% Decode arbitrary JSON, `undefined` for an empty or malformed body. OTP's
%% decoder raises on malformed input and rejects the non-JSON NaN/Infinity
%% tokens.
-spec decode(binary() | undefined) -> term() | undefined.
decode(undefined) ->
    undefined;
decode(<<>>) ->
    undefined;
decode(Bin) when is_binary(Bin) ->
    try
        json:decode(Bin)
    catch
        error:_ -> undefined
    end.

-doc false.
-spec get(term(), binary()) -> term() | undefined.
get(#{} = M, Key) ->
    case maps:get(Key, M, undefined) of
        null -> undefined;
        V -> V
    end;
get(_, _) ->
    undefined.

-doc false.
-spec str(term()) -> binary().
str(B) when is_binary(B) -> B;
str(_) -> <<>>.

-doc false.
%% A JSON number as a float, `0.0` when absent or not a number (JSON booleans
%% are atoms here, so they are never counted as numbers).
-spec num(term()) -> float().
num(N) when is_number(N) -> float(N);
num(_) -> 0.0.

-doc false.
%% A JSON number truncated toward zero to an integer, `0` when absent.
-spec int(term()) -> integer().
int(N) -> trunc(num(N)).

-doc false.
-spec list(term()) -> list() | undefined.
list(L) when is_list(L) -> L;
list(_) -> undefined.

-doc false.
%% JSON truthiness: false/null/absent, 0, "" and empty arrays/objects are
%% false.
-spec truthy(term()) -> boolean().
truthy(undefined) -> false;
truthy(null) -> false;
truthy(false) -> false;
truthy(<<>>) -> false;
truthy([]) -> false;
truthy(M) when M =:= #{} -> false;
truthy(N) when is_number(N) -> N /= 0;
truthy(_) -> true.

-doc false.
%% A human message out of a decoded vendor error body across the common
%% shapes: a string, `{error: string}`, `{error: {message}}`, `{message}`,
%% `{errors: [{message}]}`.
-spec error_message(term()) -> binary().
error_message(B) when is_binary(B) ->
    B;
error_message(#{} = Body) ->
    case maps:get(<<"error">>, Body, undefined) of
        E when is_binary(E) ->
            E;
        #{<<"message">> := M} when is_binary(M) ->
            M;
        _ ->
            case Body of
                #{<<"message">> := M} when is_binary(M) ->
                    M;
                #{<<"errors">> := [#{<<"message">> := M} | _]} when is_binary(M) ->
                    M;
                _ ->
                    <<>>
            end
    end;
error_message(_) ->
    <<>>.
