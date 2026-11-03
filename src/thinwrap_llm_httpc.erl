-module(thinwrap_llm_httpc).
-moduledoc """
Default transport, built on OTP `httpc`. Implements both `request/2`
(buffered) and `stream/4` (incremental, used by `thinwrap_llm:stream/4`).

Options (all optional), passed as `transport => {thinwrap_llm_httpc, Opts}`:

- `timeout` — milliseconds (default `600000`, ten minutes). For `request/2` it
  is the whole-request budget: a non-streaming reasoning call sends nothing
  until the model has finished, so a shorter default would cut long answers
  off (600 s matches the official OpenAI and Anthropic SDK defaults). For
  `stream/4` it is the idle budget between two chunks, so a long stream that
  keeps producing tokens is never cut off. `httpc` itself defaults to
  `infinity`, so a blackholed connection would otherwise hang the calling
  process forever.
- `connect_timeout` — milliseconds (default: `timeout`).
- `profile` — an `httpc` profile you started yourself (default: `default`).
  The library never starts a profile or any long-lived process (see
  `stream/4` below for the one short-lived helper).
- `ssl` — a **complete** list of `ssl` client options. Leave it out: since
  OTP 26 `httpc` verifies the peer and its host name against the OS trust
  store by default, and passing *any* `ssl` list replaces those defaults
  rather than adding to them.

The adapter is HTTPS-only: an `http://` (or any other) URL is refused with an
`invalid_request` error instead of being sent in cleartext. The `http://`
defaults of the local servers (vLLM, Ollama, LM Studio) therefore need your
own transport. Redirects are never followed.

`stream/4` runs `httpc` in asynchronous streaming mode (`{sync, false}`,
`{stream, self}`) inside one helper process per call, spawned and monitored
by the caller, which forwards each event to the caller; the fold itself runs
in the calling process. On every exit path — end of body, a halted fold, an
idle timeout, a failure, or an exception raised by the fold — the helper
cancels the request and the caller waits for the helper's `'DOWN'`, dropping
whatever it forwarded meanwhile. Because messages between two processes
arrive in send order, nothing can follow that `'DOWN'`: the caller's mailbox
is left exactly as it was. The helper also monitors the caller and cancels
the request if the caller dies, so it never outlives the call. An exception
from the fold propagates unchanged after that cleanup.

**First body bytes may be held back (an OTP `httpc` limitation).** When the
first body bytes arrive in the same socket read as the response headers,
`httpc` decodes them but does not stream them: in OTP's `httpc_handler`
module, `handle_http_body` keeps an incomplete chunked body (the
`{Module, Function, Args}` continuation returned by `http_chunk`'s `decode`)
without streaming what it already decoded, and the "steal decoded chunks"
path that does stream partial bodies runs only for later reads (the
`do_handle_info` clause for socket data with a pending continuation). Checked
in inets 9.2 (OTP 27.0) and 9.8.1 (OTP 29.1); the `{stream, {self, once}}`
mode shares the same path. In practice the first SSE
event of a response can therefore reach the fold only together with the next
read, and the idle `timeout` keeps running meanwhile. A server that sends its
headers, one event, and then stays silent longer than `timeout` ends in
`{error, timeout, _}` even though that event was sent. Use your own transport
if this matters.
""".

-behaviour(thinwrap_llm_transport).

-export([request/2, stream/4]).

-define(DEFAULT_TIMEOUT_MS, 600000).
%% How long stop_helper/1 waits for the streaming helper before killing it.
-define(STOP_GRACE_MS, 5000).
%% Cloudflare-fronted vendor APIs (Groq, among others) answer a missing or
%% library-default User-Agent with 403 "error code: 1010", so the adapter
%% names itself. A User-Agent already on the request wins.
-define(USER_AGENT, "thinwrap-llm-erlang").

-doc "Send one request with `httpc`. See the module doc for `Opts`.".
-spec request(thinwrap_llm_transport:request(), map()) ->
    {ok, thinwrap_llm_transport:response()} | {error, term()}.
request(#{method := Method, url := Url, headers := _} = Req, Opts) when is_map(Opts) ->
    case uri_scheme(Url) of
        <<"https">> ->
            ok = ensure_started(),
            Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT_MS),
            HttpOpts = http_opts(Timeout, Opts),
            Request = build_request(Req),
            Profile = maps:get(profile, Opts, default),
            case httpc:request(Method, Request, HttpOpts, [{body_format, binary}], Profile) of
                %% `{body_format, binary}` makes the body a binary.
                {ok, {{_Version, Status, _Reason}, RespHeaders, RespBody}} when
                    is_binary(RespBody)
                ->
                    {ok, #{
                        status => Status, headers => headers_map(RespHeaders), body => RespBody
                    }};
                {error, Reason} ->
                    {error, Reason}
            end;
        Scheme ->
            {error, {connector_error, scheme_refused(Scheme)}}
    end;
request(Req, Opts) ->
    erlang:error(badarg, [Req, Opts]).

-doc """
Send one request and fold `Fun` over the response as it arrives. See
`thinwrap_llm_transport` for the event protocol and the module doc for `Opts`.
""".
-spec stream(
    thinwrap_llm_transport:request(), map(), thinwrap_llm_transport:stream_fun(), term()
) -> {ok, term()} | {error, term(), term()}.
stream(#{method := Method, url := Url, headers := _} = Req, Opts, Fun, Acc0) when
    is_map(Opts), is_function(Fun, 2)
->
    case uri_scheme(Url) of
        <<"https">> ->
            ok = ensure_started(),
            Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT_MS),
            %% httpc's own timeout bounds the whole exchange, which would cut a
            %% long stream off mid-answer; the idle budget is enforced in
            %% receive_loop/4 instead.
            ConnectTimeout = connect_timeout(Timeout, Opts),
            HttpOpts = http_opts(infinity, Opts#{connect_timeout => ConnectTimeout}),
            Request = build_request(Req),
            Profile = maps:get(profile, Opts, default),
            Helper = start_helper(Method, Request, HttpOpts, Profile),
            try receive_loop(Helper, Timeout, Fun, Acc0) of
                {helper_down, Result} ->
                    %% Its 'DOWN' is already consumed: nothing of it remains.
                    Result;
                Result ->
                    stop_helper(Helper),
                    Result
            catch
                Class:Reason:Stack ->
                    stop_helper(Helper),
                    erlang:raise(Class, Reason, Stack)
            end;
        Scheme ->
            {error, {connector_error, scheme_refused(Scheme)}, Acc0}
    end;
stream(Req, Opts, Fun, Acc0) ->
    erlang:error(badarg, [Req, Opts, Fun, Acc0]).

%% ---------------------------------------------------------------------------
%% The streaming helper.
%%
%% `httpc:cancel_request/2` is asynchronous (OTP documents that the response
%% may still be delivered after it), so cancelling and then flushing the
%% caller's mailbox cannot be made race-free: a message the handler sends
%% just after the flush stays behind. The request is therefore owned by a
%% helper process, one per call, that is the `{stream, self}` receiver and
%% forwards each event to the caller as `{Ref, Event}`. To end the call the
%% caller tells the helper to stop and drains `{Ref, _}` until the helper's
%% 'DOWN': messages between two processes arrive in the order they were
%% sent, so nothing the helper sent can arrive after its 'DOWN', and
%% httpc's own messages only ever reach the helper.
%%
%% `{stream, self}` (not `{self, once}`) keeps the original flow: httpc reads
%% as fast as the server sends, and the idle budget is measured in the caller
%% between two events, exactly as before. `{self, once}` would add a round
%% trip per chunk and change neither the mailbox guarantee (that comes from
%% the 'DOWN' ordering) nor the held first chunk (see the module doc).

start_helper(Method, Request, HttpOpts, Profile) ->
    Caller = self(),
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(fun() ->
        helper(Caller, Ref, Method, Request, HttpOpts, Profile)
    end),
    {Pid, MRef, Ref}.

helper(Caller, Ref, Method, Request, HttpOpts, Profile) ->
    %% The helper never outlives the call: if the caller dies (killed
    %% mid-stream), the request is cancelled and the helper ends.
    CallerMon = erlang:monitor(process, Caller),
    StreamOpts = [{sync, false}, {stream, self}, {body_format, binary}],
    try httpc:request(Method, Request, HttpOpts, StreamOpts, Profile) of
        {ok, Id} ->
            forward({Caller, CallerMon, Ref}, Id, Profile);
        {error, Reason} ->
            Caller ! {Ref, {error, Reason}}
    catch
        %% Re-raised in the caller, where httpc:request/5 used to run.
        Class:Reason:Stack ->
            Caller ! {Ref, {raised, Class, Reason, Stack}}
    end.

forward({Caller, CallerMon, Ref} = To, Id, Profile) ->
    receive
        {http, {Id, stream_start, Headers}} ->
            %% httpc streams only a 200 (or 206) body; every other status
            %% arrives as one complete response below.
            Caller ! {Ref, {status, 200, headers_map(Headers)}},
            forward(To, Id, Profile);
        {http, {Id, stream, Chunk}} ->
            Caller ! {Ref, {data, Chunk}},
            forward(To, Id, Profile);
        {http, {Id, stream_end, _Trailers}} ->
            Caller ! {Ref, done};
        {http, {Id, {{_Version, Status, _Reason}, Headers, Body}}} ->
            Caller ! {Ref, {response, Status, headers_map(Headers), Body}};
        {http, {Id, {error, Reason}}} ->
            Caller ! {Ref, {error, Reason}};
        {Ref, stop} ->
            _ = httpc:cancel_request(Id, Profile),
            ok;
        {'DOWN', CallerMon, process, Caller, _} ->
            _ = httpc:cancel_request(Id, Profile),
            ok
    after infinity ->
        %% No timer of its own: the caller enforces the idle budget and stops
        %% the helper, and the monitor above covers a caller that dies.
        ok
    end.

%% Waits for the helper's 'DOWN', dropping whatever it forwarded meanwhile.
%% A helper still blocked inside httpc:request/5 (no request id yet) cannot
%% read `stop`; after a grace period it is killed, which always yields the
%% 'DOWN'.
stop_helper({Pid, MRef, Ref}) ->
    Pid ! {Ref, stop},
    drain(Pid, MRef, Ref, ?STOP_GRACE_MS).

drain(Pid, MRef, Ref, Grace) ->
    receive
        {Ref, _} ->
            drain(Pid, MRef, Ref, Grace);
        {'DOWN', MRef, process, Pid, _} ->
            ok
    after Grace ->
        exit(Pid, kill),
        drain(Pid, MRef, Ref, infinity)
    end.

receive_loop({Pid, MRef, Ref} = Helper, Timeout, Fun, Acc) ->
    receive
        {Ref, {status, Status, Headers}} ->
            step(Fun({status, Status, Headers}, Acc), Helper, Timeout, Fun);
        {Ref, {data, Chunk}} ->
            step(Fun({data, Chunk}, Acc), Helper, Timeout, Fun);
        {Ref, done} ->
            {ok, Acc};
        {Ref, {response, Status, Headers, Body}} ->
            case Fun({status, Status, Headers}, Acc) of
                {cont, Acc1} when Body =:= <<>> ->
                    {ok, Acc1};
                {cont, Acc1} ->
                    {_, Acc2} = Fun({data, Body}, Acc1),
                    {ok, Acc2};
                {halt, Acc1} ->
                    {ok, Acc1}
            end;
        {Ref, {error, Reason}} ->
            {error, Reason, Acc};
        {Ref, {raised, Class, Reason, Stack}} ->
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MRef, process, Pid, Reason} ->
            %% Only if something outside the library killed the helper.
            {helper_down, {error, Reason, Acc}}
    after Timeout ->
        {error, timeout, Acc}
    end.

step({cont, Acc}, Helper, Timeout, Fun) -> receive_loop(Helper, Timeout, Fun, Acc);
step({halt, Acc}, _Helper, _Timeout, _Fun) -> {ok, Acc}.

http_opts(Timeout, Opts) ->
    HttpOpts0 = [
        {timeout, Timeout},
        {connect_timeout, connect_timeout(Timeout, Opts)},
        {autoredirect, false}
    ],
    case Opts of
        #{ssl := Ssl} -> [{ssl, Ssl} | HttpOpts0];
        _ -> HttpOpts0
    end.

connect_timeout(Timeout, Opts) -> maps:get(connect_timeout, Opts, Timeout).

build_request(#{method := Method, url := Url, headers := Headers0} = Req) ->
    {ContentType, Headers} = split_content_type(with_user_agent(Headers0)),
    HttpcHeaders = [{to_list(K), to_list(V)} || {K, V} <- Headers],
    case Method of
        get -> {Url, HttpcHeaders};
        post -> {Url, HttpcHeaders, ContentType, body_or_empty(maps:get(body, Req, undefined))}
    end.

%% `inets`/`ssl` are listed in the app's `applications`, so a release or a Mix
%% project has them running; this covers a bare `erl -pa` / escript caller and
%% is a no-op otherwise.
ensure_started() ->
    case application:ensure_all_started([inets, ssl]) of
        {ok, _} -> ok;
        {error, _} = E -> erlang:error(E)
    end.

uri_scheme(Url) ->
    case uri_string:parse(Url) of
        #{scheme := S} when is_binary(S) -> string:lowercase(S);
        %% A charlist URL is outside the request() contract (`url` is a
        %% binary); it fails as building the refusal message from it would.
        #{scheme := _} -> erlang:error(badarg);
        _ -> <<>>
    end.

%% A configuration problem rather than an unreachable provider, so a typed
%% invalid_request that says what to do. The URL is never echoed: a
%% self-hosted base URL may carry userinfo credentials.
scheme_refused(<<"http">>) ->
    thinwrap_llm_error:invalid_request(
        <<
            "The default transport is HTTPS-only and refuses http:// URLs. To reach a "
            "cleartext http endpoint (e.g. a local vLLM, Ollama or LM Studio server), "
            "pass your own transport in the config's transport key."
        >>
    );
scheme_refused(Scheme) ->
    Shown =
        case Scheme of
            <<>> -> <<"(none)">>;
            _ -> Scheme
        end,
    thinwrap_llm_error:invalid_request(
        <<"The default transport only sends https:// URLs (got scheme: ", Shown/binary,
            "). Pass your own transport in the config's transport key for anything else.">>
    ).

with_user_agent(Headers) ->
    case lists:any(fun(K) -> string:lowercase(K) =:= <<"user-agent">> end, maps:keys(Headers)) of
        true -> Headers;
        false -> Headers#{<<"User-Agent">> => <<?USER_AGENT>>}
    end.

%% httpc takes the content type as a separate request element and adds the
%% header itself; leaving it in the header list too would send it twice.
split_content_type(Headers) ->
    maps:fold(
        fun(K, V, {CT, Acc}) ->
            case string:lowercase(K) of
                <<"content-type">> -> {to_list(V), Acc};
                _ -> {CT, [{K, V} | Acc]}
            end
        end,
        {"application/octet-stream", []},
        Headers
    ).

body_or_empty(undefined) -> <<>>;
body_or_empty(B) -> B.

%% httpc already lower-cases response header names; a repeated header arrives
%% as separate entries and is joined the way RFC 9110 allows.
headers_map(List) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            %% httpc header text is a byte list, every element a code point.
            Key = string:lowercase(thinwrap_llm_util:bin(K)),
            Val = thinwrap_llm_util:bin(V),
            case Acc of
                #{Key := Prev} -> Acc#{Key := <<Prev/binary, ", ", Val/binary>>};
                _ -> Acc#{Key => Val}
            end
        end,
        #{},
        List
    ).

to_list(B) when is_binary(B) -> unicode:characters_to_list(B);
to_list(L) when is_list(L) -> L.
