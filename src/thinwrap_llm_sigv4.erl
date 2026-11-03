-module(thinwrap_llm_sigv4).
-moduledoc false.

%% Hand-rolled AWS Signature Version 4 (HMAC-SHA256) on OTP `crypto` — no
%% third-party dependency. Used by the Bedrock native adapter.

-export([sign/1, canonical_query/1]).

-export_type([params/0]).

-type params() :: #{
    method := binary(),
    service := binary(),
    region := binary(),
    host := binary(),
    path := binary(),
    payload := binary(),
    access_key_id := binary(),
    secret_access_key := binary(),
    session_token => binary(),
    canonical_query => binary(),
    signed => #{binary() => binary()},
    %% Unix seconds; the current time when absent (a test seam).
    now => integer()
}.

%% Headers AWS never signs (hop-by-hop, or rewritten by intermediaries).
-define(UNSIGNABLE, [
    <<"authorization">>,
    <<"cache-control">>,
    <<"connection">>,
    <<"content-length">>,
    <<"expect">>,
    <<"from">>,
    <<"keep-alive">>,
    <<"max-forwards">>,
    <<"pragma">>,
    <<"proxy-authenticate">>,
    <<"proxy-authorization">>,
    <<"referer">>,
    <<"te">>,
    <<"trailer">>,
    <<"transfer-encoding">>,
    <<"upgrade">>,
    <<"user-agent">>,
    <<"x-amzn-trace-id">>
]).

-doc false.
%% The headers to add to the request: Host, X-Amz-Date, X-Amz-Content-Sha256,
%% Authorization and (with a session token) X-Amz-Security-Token.
-spec sign(params()) -> #{binary() => binary()}.
sign(
    #{
        method := Method,
        service := Service,
        region := Region,
        host := Host,
        path := Path,
        payload := Payload,
        access_key_id := AccessKeyId,
        secret_access_key := Secret
    } = P
) ->
    Now = maps:get(now, P, erlang:system_time(second)),
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Now, second),
    DateStamp = iolist_to_binary(io_lib:format("~4..0B~2..0B~2..0B", [Y, Mo, D])),
    AmzDate = iolist_to_binary(
        io_lib:format("~sT~2..0B~2..0B~2..0BZ", [DateStamp, H, Mi, S])
    ),
    HashedPayload = hex(crypto:hash(sha256, Payload)),
    Token = maps:get(session_token, P, <<>>),
    Canonical0 = maps:fold(
        fun(K, V, Acc) -> Acc#{string:lowercase(K) => collapse(V)} end,
        #{},
        maps:get(signed, P, #{})
    ),
    Canonical1 = Canonical0#{
        <<"host">> => collapse(Host),
        <<"x-amz-date">> => AmzDate,
        <<"x-amz-content-sha256">> => HashedPayload
    },
    Canonical2 =
        case Token of
            <<>> -> Canonical1;
            _ -> Canonical1#{<<"x-amz-security-token">> => Token}
        end,
    Canonical = maps:without(?UNSIGNABLE, Canonical2),
    Names = lists:sort(maps:keys(Canonical)),
    CanonicalHeaders = iolist_to_binary([[N, ":", maps:get(N, Canonical), "\n"] || N <- Names]),
    SignedHeaders = iolist_to_binary(lists:join(<<";">>, Names)),
    CanonicalRequest = iolist_to_binary(
        lists:join(<<"\n">>, [
            Method,
            Path,
            maps:get(canonical_query, P, <<>>),
            CanonicalHeaders,
            SignedHeaders,
            HashedPayload
        ])
    ),
    Scope = <<DateStamp/binary, "/", Region/binary, "/", Service/binary, "/aws4_request">>,
    StringToSign = iolist_to_binary(
        lists:join(<<"\n">>, [
            <<"AWS4-HMAC-SHA256">>, AmzDate, Scope, hex(crypto:hash(sha256, CanonicalRequest))
        ])
    ),
    KDate = hmac(<<"AWS4", Secret/binary>>, DateStamp),
    KRegion = hmac(KDate, Region),
    KService = hmac(KRegion, Service),
    KSigning = hmac(KService, <<"aws4_request">>),
    Signature = hex(hmac(KSigning, StringToSign)),
    Authorization =
        <<"AWS4-HMAC-SHA256 Credential=", AccessKeyId/binary, "/", Scope/binary, ", SignedHeaders=",
            SignedHeaders/binary, ", Signature=", Signature/binary>>,
    Out = #{
        <<"Host">> => Host,
        <<"X-Amz-Date">> => AmzDate,
        <<"X-Amz-Content-Sha256">> => HashedPayload,
        <<"Authorization">> => Authorization
    },
    case Token of
        <<>> -> Out;
        _ -> Out#{<<"X-Amz-Security-Token">> => Token}
    end.

-doc false.
%% The canonical query string: keys sorted, keys and values RFC 3986-encoded.
%% The same bytes go on the wire, so the signature the server recomputes from
%% the received query matches.
-spec canonical_query(#{binary() => binary() | atom() | number()}) -> binary().
canonical_query(Query) ->
    iolist_to_binary(
        lists:join(<<"&">>, [
            [
                thinwrap_llm_util:rfc3986(K),
                "=",
                thinwrap_llm_util:rfc3986(thinwrap_llm_util:bin(maps:get(K, Query)))
            ]
         || K <- lists:sort(maps:keys(Query))
        ])
    ).

collapse(V) ->
    re:replace(string:trim(V), "\\s+", " ", [global, unicode, ucp, {return, binary}]).

hmac(Key, Data) -> crypto:mac(hmac, sha256, Key, Data).

hex(Bin) -> binary:encode_hex(Bin, lowercase).
