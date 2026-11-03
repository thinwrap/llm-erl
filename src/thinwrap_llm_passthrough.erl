-module(thinwrap_llm_passthrough).
-moduledoc false.

%% The per-request escape hatch for sub-baseline / provider-specific fields,
%% forwarded verbatim and never emulated. `body` is SHALLOW-merged onto (and
%% overrides) the connector-built body: a passthrough key replaces the
%% connector's whole value for that key. `headers` are merged onto the request
%% headers (caller wins); `query` is appended to the URL. Body and query keys
%% may be atoms or binaries; they go on the wire as text.

-export([apply_body/2, merge_headers/2, query/1, append_query/2]).

-doc false.
-spec apply_body(map(), map()) -> map().
apply_body(Body, Input) ->
    case pt(Input, body) of
        #{} = PtBody -> maps:merge(Body, thinwrap_llm_util:keys_to_binary(PtBody));
        undefined -> Body
    end.

-doc false.
%% Exact-name merge, as the sibling libraries do: names are not folded to one
%% case here (Bedrock, whose signature depends on it, merges case-insensitively
%% itself).
-spec merge_headers(map(), map()) -> map().
merge_headers(Base, Input) ->
    case pt(Input, headers) of
        #{} = H -> maps:merge(Base, header_map(H));
        undefined -> Base
    end.

-doc false.
%% The caller's passthrough query as a map with binary keys (empty when none).
-spec query(map()) -> #{binary() => binary() | atom() | number()}.
query(Input) ->
    case pt(Input, query) of
        #{} = Q -> maps:from_list([{thinwrap_llm_util:bin(K), V} || K := V <- Q]);
        undefined -> #{}
    end.

-doc false.
%% `Url` with the passthrough query appended, using `&` when the URL already
%% carries a query string (Gemini's streaming `?alt=sse`).
-spec append_query(binary(), map()) -> binary().
append_query(Url, Input) ->
    Q = query(Input),
    case map_size(Q) of
        0 ->
            Url;
        _ ->
            Sep =
                case binary:match(Url, <<"?">>) of
                    nomatch -> <<"?">>;
                    _ -> <<"&">>
                end,
            <<Url/binary, Sep/binary, (thinwrap_llm_util:urlencode(Q))/binary>>
    end.

pt(Input, Key) ->
    case maps:get(passthrough, Input, #{}) of
        #{Key := V} when map_size(V) > 0 -> V;
        _ -> undefined
    end.

header_map(H) ->
    maps:from_list([{thinwrap_llm_util:bin(K), thinwrap_llm_util:bin(V)} || K := V <- H]).
