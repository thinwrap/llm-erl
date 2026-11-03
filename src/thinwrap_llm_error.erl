-module(thinwrap_llm_error).
-moduledoc """
Builders for `t:thinwrap_llm:connector_error/0`, the plain map every
operation failure is returned as.

Connectors build errors here and `fail/1` them; the public `thinwrap_llm`
functions catch that and return `{error, Error}`. Misuse of the API (a missing
config key, a bad enum atom) is not a connector error: it raises `badarg`.

There is deliberately no top-level retry key: for a vendor HTTP error the raw
`Retry-After` header rides in `cause` as `<<"retryAfter">>` and its parsed
seconds as `<<"retryAfterSeconds">>`; the decoded vendor body is
`<<"raw">>`.
""".

-export([
    new/2,
    invalid_request/1,
    vendor_error/5,
    empty_body/1,
    malformed_body/3,
    stream_ended_early/0,
    embedded_error_status/1,
    embedded_error_class/1,
    parse_retry_after/1,
    parse_retry_after/2,
    fail/1,
    codes/0
]).

-export_type([code/0]).

-doc """
Normalized, cross-provider error classification; the same values as the
TypeScript, PHP, Go and Python siblings.
""".
-type code() ::
    rate_limited
    | auth_failed
    | provider_unavailable
    | invalid_request
    | context_length_exceeded
    | content_filtered
    | unknown.

-define(THROW_TAG, thinwrap_llm_error).

-doc "Every `code()` value, in declaration order.".
-spec codes() -> [code()].
codes() ->
    [
        rate_limited,
        auth_failed,
        provider_unavailable,
        invalid_request,
        context_length_exceeded,
        content_filtered,
        unknown
    ].

-doc """
Build an error. `Fields` may carry `status`, `provider_message`, `message`
and `cause`; `message` falls back to `provider_message`, then to a generic
text, so it is always present. `undefined` values are dropped.
""".
-spec new(code(), map()) -> thinwrap_llm:connector_error().
new(Code, Fields) ->
    Message =
        case Fields of
            #{message := M} when is_binary(M), M =/= <<>> -> M;
            #{provider_message := P} when is_binary(P), P =/= <<>> -> P;
            _ -> <<"Connector error">>
        end,
    Kept = maps:filter(
        fun(_K, V) -> V =/= undefined end, maps:with([status, provider_message, cause], Fields)
    ),
    maps:merge(#{code => Code, message => Message}, Kept).

-doc "A pre-flight `invalid_request` (no HTTP round-trip).".
-spec invalid_request(binary()) -> thinwrap_llm:connector_error().
invalid_request(Msg) ->
    new(invalid_request, #{message => Msg, provider_message => Msg}).

-doc """
The error for a non-2xx response (or an error object inside a 2xx body or a
stream frame, which keeps the status it arrived with). The status-to-code
classification and the message are the connector's; this adds the shared
`Retry-After` surfacing.
""".
-spec vendor_error(
    integer(), thinwrap_llm_transport:headers(), term(), code(), binary() | undefined
) -> thinwrap_llm:connector_error().
vendor_error(Status, Headers, RawBody, Code, ProviderMsg) ->
    Cause0 = #{<<"raw">> => raw(RawBody)},
    RetryAfter = string:trim(maps:get(<<"retry-after">>, Headers, <<>>)),
    Cause =
        case RetryAfter of
            <<>> ->
                Cause0;
            _ ->
                C1 = Cause0#{<<"retryAfter">> => RetryAfter},
                case parse_retry_after(RetryAfter) of
                    undefined -> C1;
                    Secs -> C1#{<<"retryAfterSeconds">> => Secs}
                end
        end,
    Msg =
        case ProviderMsg of
            M when is_binary(M), M =/= <<>> -> M;
            _ -> <<"HTTP ", (integer_to_binary(Status))/binary>>
        end,
    new(Code, #{status => Status, provider_message => Msg, message => Msg, cause => Cause}).

raw(undefined) -> null;
raw(V) -> V.

-doc """
A 2xx response with no decodable JSON body (a proxy or captive-portal HTML
page, a truncated read). Without this guard the connector would parse nothing
into an empty "successful" result.
""".
-spec empty_body(integer()) -> thinwrap_llm:connector_error().
empty_body(Status) ->
    Msg = <<"provider returned a successful status with an empty or non-JSON body">>,
    new(provider_unavailable, #{status => Status, message => Msg, provider_message => Msg}).

-doc """
A 2xx JSON body that decodes but cannot be a real result (one embedding per
input missing, a vector cell that is not a number). Coercing it would hand the
caller silently wrong data.
""".
-spec malformed_body(integer(), term(), binary()) -> thinwrap_llm:connector_error().
malformed_body(Status, RawBody, Msg) ->
    new(provider_unavailable, #{
        status => Status,
        message => Msg,
        provider_message => Msg,
        cause => #{<<"raw">> => raw(RawBody)}
    }).

-doc """
A stream whose body ended before the provider's terminal event. Every
supported provider closes a complete stream with one, so an EOF before it is
a truncated answer (dropped connection, proxy timeout), not a short one.
""".
-spec stream_ended_early() -> thinwrap_llm:connector_error().
stream_ended_early() ->
    Msg = <<"Stream ended before the provider sent its terminal event">>,
    new(provider_unavailable, #{message => Msg, provider_message => Msg}).

%% HTTP status for the OpenAI-style error `code` / `type` names an embedded
%% error can carry instead of a number (`insufficient_quota` is OpenAI's
%% out-of-credit code, which it also sends as HTTP 429).
error_name_status(<<"invalid_request_error">>) -> 400;
error_name_status(<<"context_length_exceeded">>) -> 400;
error_name_status(<<"authentication_error">>) -> 401;
error_name_status(<<"invalid_api_key">>) -> 401;
error_name_status(<<"permission_error">>) -> 403;
error_name_status(<<"not_found_error">>) -> 404;
error_name_status(<<"rate_limit_error">>) -> 429;
error_name_status(<<"rate_limit_exceeded">>) -> 429;
error_name_status(<<"insufficient_quota">>) -> 429;
error_name_status(<<"server_error">>) -> 500;
error_name_status(<<"api_error">>) -> 500;
error_name_status(<<"overloaded_error">>) -> 529;
error_name_status(_) -> undefined.

-doc """
The HTTP status an error object delivered inside a 2xx body or a stream frame
carries: OpenRouter's post-processing errors put the real status in
`error.code` (e.g. 502, as a number or a three-digit string). Anything else is
not a status.
""".
-spec embedded_error_status(term()) -> 400..599 | undefined.
embedded_error_status(#{} = Err) ->
    Code =
        case maps:get(<<"code">>, Err, undefined) of
            <<A, B, C>> = S when
                A >= $0, A =< $9, B >= $0, B =< $9, C >= $0, C =< $9
            ->
                binary_to_integer(S);
            Other ->
                Other
        end,
    case Code of
        N when is_integer(N), N >= 400, N =< 599 -> N;
        _ -> undefined
    end;
embedded_error_status(_) ->
    undefined.

-doc """
The status to classify an embedded error object by: its HTTP status when it
carries one, else the status its `code` / `type` name stands for
(`insufficient_quota` → 429). `undefined` when it names nothing known.
""".
-spec embedded_error_class(term()) -> integer() | undefined.
embedded_error_class(Err) ->
    case embedded_error_status(Err) of
        undefined when is_map(Err) ->
            Code = maps:get(<<"code">>, Err, undefined),
            first_named([Code, maps:get(<<"type">>, Err, undefined)]);
        Status ->
            Status
    end.

first_named([]) ->
    undefined;
first_named([Name | Rest]) when is_binary(Name) ->
    case error_name_status(Name) of
        undefined -> first_named(Rest);
        S -> S
    end;
first_named([_ | Rest]) ->
    first_named(Rest).

-doc "`parse_retry_after/2` against the current time.".
-spec parse_retry_after(binary()) -> non_neg_integer() | undefined.
parse_retry_after(Value) ->
    parse_retry_after(Value, erlang:system_time(second)).

-doc """
Seconds to wait per RFC 9110: delay-seconds is a non-negative integer, so
`1.5` or `-5` is ignored; an HTTP-date (IMF-fixdate, RFC 850 or asctime,
matched strictly so free text such as `tomorrow` never reads as a date) gives
the time until it, and a date already past means "retry now" (0). Anything
else is `undefined`. `NowSecs` is Unix seconds.
""".
-spec parse_retry_after(binary(), integer()) -> non_neg_integer() | undefined.
parse_retry_after(Value, NowSecs) ->
    S = string:trim(Value),
    case S of
        <<>> ->
            undefined;
        _ ->
            case is_ascii_digits(S) of
                true ->
                    binary_to_integer(S);
                false ->
                    case parse_http_date(S, NowSecs) of
                        undefined -> undefined;
                        When -> max(When - NowSecs, 0)
                    end
            end
    end.

is_ascii_digits(<<>>) -> false;
is_ascii_digits(B) -> lists:all(fun(C) -> C >= $0 andalso C =< $9 end, binary_to_list(B)).

-define(IMF_RE,
    "^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), (\\d{2}) ([A-Za-z]{3}) (\\d{4}) "
    "(\\d{2}):(\\d{2}):(\\d{2}) GMT$"
).
-define(RFC850_RE,
    "^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
    "(\\d{2})-([A-Za-z]{3})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2}) GMT$"
).
-define(ASCTIME_RE,
    "^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) ([A-Za-z]{3}) ([ \\d]\\d) "
    "(\\d{2}):(\\d{2}):(\\d{2}) (\\d{4})$"
).

parse_http_date(S, NowSecs) ->
    Opts = [{capture, all_but_first, binary}],
    case re:run(S, ?IMF_RE, Opts) of
        {match, [Day, Mon, Year, H, Mi, Se]} ->
            to_unix(Year, Mon, Day, H, Mi, Se);
        nomatch ->
            case re:run(S, ?RFC850_RE, Opts) of
                {match, [Day, Mon, YY, H, Mi, Se]} ->
                    %% RFC 9110: a two-digit year more than 50 years ahead is in
                    %% the past.
                    {{NowYear, _, _}, _} = calendar:system_time_to_universal_time(
                        NowSecs, second
                    ),
                    Y0 = NowYear div 100 * 100 + binary_to_integer(YY),
                    Year =
                        case Y0 > NowYear + 50 of
                            true -> Y0 - 100;
                            false -> Y0
                        end,
                    to_unix(integer_to_binary(Year), Mon, Day, H, Mi, Se);
                nomatch ->
                    case re:run(S, ?ASCTIME_RE, Opts) of
                        {match, [Mon, Day, H, Mi, Se, Year]} ->
                            to_unix(Year, Mon, string:trim(Day), H, Mi, Se);
                        nomatch ->
                            undefined
                    end
            end
    end.

to_unix(Year, Mon, Day, H, Mi, Se) ->
    case month(string:lowercase(Mon)) of
        undefined ->
            undefined;
        M ->
            [Y, D, Hh, Mm, Ss] = [binary_to_integer(X) || X <- [Year, Day, H, Mi, Se]],
            case calendar:valid_date(Y, M, D) andalso Hh < 24 andalso Mm < 60 andalso Ss < 60 of
                true ->
                    calendar:datetime_to_gregorian_seconds({{Y, M, D}, {Hh, Mm, Ss}}) -
                        62167219200;
                false ->
                    undefined
            end
    end.

month(<<"jan">>) -> 1;
month(<<"feb">>) -> 2;
month(<<"mar">>) -> 3;
month(<<"apr">>) -> 4;
month(<<"may">>) -> 5;
month(<<"jun">>) -> 6;
month(<<"jul">>) -> 7;
month(<<"aug">>) -> 8;
month(<<"sep">>) -> 9;
month(<<"oct">>) -> 10;
month(<<"nov">>) -> 11;
month(<<"dec">>) -> 12;
month(_) -> undefined.

-doc """
Abort the current operation with `Error`. Only valid inside a call made
through `thinwrap_llm`, which turns it into `{error, Error}`.
""".
-spec fail(thinwrap_llm:connector_error()) -> no_return().
fail(Error) ->
    throw({?THROW_TAG, Error}).
