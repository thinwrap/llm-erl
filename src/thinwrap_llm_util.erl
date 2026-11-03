-module(thinwrap_llm_util).
-moduledoc false.

%% Small shared helpers: URL encoding, header-map helpers, optional-key maps.
%% Connectors call these as building blocks; nothing here intercepts a request
%% globally.

-export([
    ok_status/1,
    urlencode/1,
    rfc3986/1,
    bin/1,
    put_opt/3,
    set_header_ci/3,
    keys_to_binary/1,
    first_non_empty/1
]).

-doc false.
-spec ok_status(integer()) -> boolean().
ok_status(S) -> S >= 200 andalso S < 300.

-doc false.
%% `application/x-www-form-urlencoded` query text with the exact byte rules of
%% the sibling libraries (unreserved `A-Z a-z 0-9 _ . - ~` kept, space as `+`,
%% everything else `%XX`). `uri_string:compose_query/1` differs on `*` and `~`.
-spec urlencode(#{binary() | atom() => binary() | atom() | number()}) -> binary().
urlencode(Query) ->
    Pairs = [
        <<(quote(bin(K), true))/binary, "=", (quote(bin(V), true))/binary>>
     || {K, V} <- lists:sort([{bin(K0), V0} || K0 := V0 <- Query])
    ],
    iolist_to_binary(lists:join(<<"&">>, Pairs)).

-doc false.
%% RFC 3986 percent-encoding: only `A-Z a-z 0-9 - . _ ~` stay literal (the
%% encoding AWS SigV4 canonicalization requires).
-spec rfc3986(binary()) -> binary().
rfc3986(B) -> quote(B, false).

quote(B, PlusForSpace) ->
    <<<<(enc(C, PlusForSpace))/binary>> || <<C>> <= B>>.

enc(C, _) when C >= $a, C =< $z -> <<C>>;
enc(C, _) when C >= $A, C =< $Z -> <<C>>;
enc(C, _) when C >= $0, C =< $9 -> <<C>>;
enc(C, _) when C =:= $-; C =:= $.; C =:= $_; C =:= $~ -> <<C>>;
enc($\s, true) -> <<"+">>;
enc(C, _) -> list_to_binary(io_lib:format("%~2.16.0B", [C])).

-doc false.
-spec bin(binary() | atom() | integer() | float() | string()) -> binary().
bin(B) when is_binary(B) -> B;
bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(I) when is_integer(I) -> integer_to_binary(I);
bin(F) when is_float(F) -> float_to_binary(F, [short]);
bin(L) when is_list(L) -> converted(unicode:characters_to_binary(L)).

%% Only the success shape is accepted: a list of invalid code points is a bug
%% in the caller, not a value to pass on.
-spec converted(binary() | {error | incomplete, binary(), term()}) -> binary().
converted(B) when is_binary(B) -> B.

-doc false.
%% Add an optional result key only when there is a value: absent optional
%% output is an omitted key, never `undefined`/`null`. An empty binary counts
%% as "no value", as the siblings' empty-string defaults do.
-spec put_opt(atom(), term(), map()) -> map().
put_opt(_K, undefined, M) -> M;
put_opt(_K, <<>>, M) -> M;
put_opt(K, V, M) -> M#{K => V}.

-doc false.
%% Set a header, first dropping any entry whose name differs only in case, so
%% each header appears once whichever casing each source used (last wins).
-spec set_header_ci(map(), binary(), binary()) -> map().
set_header_ci(Headers, Name, Value) ->
    Lower = string:lowercase(Name),
    Kept = maps:filter(fun(K, _) -> string:lowercase(K) =/= Lower end, Headers),
    Kept#{Name => Value}.

-doc false.
%% Map keys at every depth (inside nested maps and lists) become binaries.
-spec keys_to_binary(map()) -> map().
keys_to_binary(M) ->
    maps:fold(fun(K, V, Acc) -> Acc#{key(K) => deep_keys_to_binary(V)} end, #{}, M).

deep_keys_to_binary(M) when is_map(M) -> keys_to_binary(M);
deep_keys_to_binary(L) when is_list(L) -> [deep_keys_to_binary(V) || V <- L];
deep_keys_to_binary(V) -> V.

key(K) when is_atom(K); is_binary(K); is_integer(K) -> bin(K);
key(K) -> K.

-doc false.
-spec first_non_empty([binary()]) -> binary().
first_non_empty([]) -> <<>>;
first_non_empty([<<>> | Rest]) -> first_non_empty(Rest);
first_non_empty([V | _]) -> V.
