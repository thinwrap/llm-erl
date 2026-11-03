-module(thinwrap_llm_httpc_tests).

%% The default transport against a real local TLS listener (a throwaway CA
%% from public_key:pkix_test_data/1), so the adapter's own behaviour — status
%% passthrough, header shape, no redirects, timeouts, User-Agent, incremental
%% streaming and its mailbox hygiene — is tested without the network.

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(T, thinwrap_llm_httpc).

httpc_test_() ->
    {setup, fun start/0, fun stop/1, fun(Ctx) ->
        [
            {"2xx returns status, lower-cased joined headers and body", fun() ->
                ok_response(Ctx)
            end},
            {"non-2xx is a response, not an error", fun() -> error_status(Ctx) end},
            {"a redirect is surfaced, never followed", fun() -> redirect(Ctx) end},
            {"names itself in User-Agent; a caller's wins", fun() -> user_agent(Ctx) end},
            {"POST sends body and one content type", fun() -> post(Ctx) end},
            {"a hung server is {error, timeout}", fun() -> hung(Ctx) end},
            {"refuses non-https URLs with invalid_request", fun refuses_non_https/0},
            {"default TLS rejects an untrusted certificate", fun() -> untrusted(Ctx) end},
            {"stream: status first, then the body in chunks", fun() -> stream_chunks(Ctx) end},
            {"stream: a non-2xx arrives as status + body", fun() -> stream_error_status(Ctx) end},
            {"stream: a halted fold stops reading, mailbox clean", fun() -> stream_halt(Ctx) end},
            {"stream: an idle server times out, mailbox clean", fun() -> stream_idle(Ctx) end},
            {"stream: a raising fold propagates, mailbox clean", fun() -> stream_raise(Ctx) end},
            {"stream: a request error is {error, _, Acc}, mailbox clean", fun() ->
                stream_request_error(Ctx)
            end},
            {"stream: the helper dies with a killed caller", fun() -> stream_caller_killed(Ctx) end},
            {"stream: User-Agent and request body", fun() -> stream_user_agent(Ctx) end},
            {"stream: refuses non-https URLs", fun stream_refuses_non_https/0},
            {"stream: connect failure is {error, _, Acc0}", fun stream_connect_failure/0},
            {"end to end: thinwrap_llm:stream/4 over TLS", fun() -> end_to_end(Ctx) end}
        ]
    end}.

start() ->
    {ok, _} = application:ensure_all_started([inets, ssl]),
    #{server_config := SOpts, client_config := COpts} =
        public_key:pkix_test_data(#{
            %% P-256/SHA-256: OTP's default test keys cannot be served to a
            %% TLS 1.3 client (unable_to_supply_acceptable_cert).
            server_chain => #{
                root => [{key, {namedCurve, secp256r1}}, {digest, sha256}],
                intermediates => [],
                peer => [{key, {namedCurve, secp256r1}}, {digest, sha256}, {extensions, [san()]}]
            },
            client_chain => #{
                root => [{key, {namedCurve, secp256r1}}],
                intermediates => [],
                peer => [{key, {namedCurve, secp256r1}}]
            }
        }),
    {ok, L} = ssl:listen(0, [binary, {active, false}, {reuseaddr, true} | SOpts]),
    {ok, {_, Port}} = ssl:sockname(L),
    Acceptor = spawn(fun() -> accept_loop(L) end),
    ok = ssl:controlling_process(L, Acceptor),
    %% The client trusts the test root and checks the host name like the default.
    Ssl = [
        {verify, verify_peer},
        {cacerts, proplists:get_value(cacerts, COpts)},
        {server_name_indication, "localhost"},
        {customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}
    ],
    #{port => Port, ssl => Ssl, acceptor => Acceptor, listen => L}.

san() ->
    {'Extension', ?'id-ce-subjectAltName', false, [{dNSName, "localhost"}]}.

stop(#{acceptor := A, listen := L}) ->
    exit(A, kill),
    _ = ssl:close(L),
    ok.

url(#{port := P}, Path) -> iolist_to_binary(["https://localhost:", integer_to_list(P), Path]).

opts(Ctx) -> #{ssl => maps:get(ssl, Ctx), timeout => 5000}.

accept_loop(L) ->
    case ssl:transport_accept(L) of
        {ok, T} ->
            case ssl:handshake(T, 5000) of
                {ok, S} ->
                    Pid = spawn(fun() -> serve(S) end),
                    _ = ssl:controlling_process(S, Pid),
                    Pid ! go;
                _ ->
                    ok
            end,
            accept_loop(L);
        _ ->
            ok
    end.

serve(S) ->
    %% `go` arrives once the acceptor has handed over the socket.
    receive
        go -> ok
    after 5000 -> erlang:error(no_socket_handoff)
    end,
    {ok, Req} = read_request(S, <<>>),
    [Head | _] = binary:split(Req, <<"\r\n\r\n">>),
    [RequestLine | HeaderLines] = binary:split(Head, <<"\r\n">>, [global]),
    [_Method, Path | _] = binary:split(RequestLine, <<" ">>, [global]),
    %% eunit runs setup and each test in different processes, so the test
    %% that wants the raw request registers itself under this name.
    case whereis(httpc_test_sink) of
        undefined -> ok;
        Sink -> Sink ! {request, Req}
    end,
    UA = hd(
        [
            V
         || Line <- HeaderLines,
            [K, V] <- [binary:split(Line, <<": ">>)],
            string:lowercase(K) =:= <<"user-agent">>
        ] ++ [<<>>]
    ),
    reply(Path, UA, S),
    ssl:close(S).

reply(<<"/ok">>, _UA, S) ->
    send(S, <<"HTTP/1.1 200 OK\r\nX-Dup: 1\r\nX-Dup: 2\r\nContent-Length: 2\r\n\r\nhi">>);
reply(<<"/teapot">>, _UA, S) ->
    send(S, <<"HTTP/1.1 418 I'm a teapot\r\nContent-Length: 5\r\n\r\nnope!">>);
reply(<<"/redirect">>, _UA, S) ->
    send(
        S, <<"HTTP/1.1 302 Found\r\nLocation: https://example.com/\r\nContent-Length: 0\r\n\r\n">>
    );
reply(<<"/echo-ua">>, UA, S) ->
    send(
        S,
        <<"HTTP/1.1 200 OK\r\nContent-Length: ", (integer_to_binary(byte_size(UA)))/binary,
            "\r\n\r\n", UA/binary>>
    );
reply(<<"/hang">>, _UA, _S) ->
    timer:sleep(3000);
reply(<<"/sse">>, _UA, S) ->
    send(S, sse_head()),
    [
        begin
            send(S, chunk(C)),
            timer:sleep(20)
        end
     || C <- [<<"data: {\"a\":">>, <<"1}\n\nda">>, <<"ta: [DONE]\n\n">>]
    ],
    send(S, <<"0\r\n\r\n">>);
reply(<<"/sse-gated">>, _UA, S) ->
    %% The first chunk goes out only once the client reported the status, so
    %% it can never share a read with the headers. httpc holds body bytes that
    %% arrive in the same read as the headers until the next read (see the
    %% thinwrap_llm_httpc module doc), which would make the idle test depend
    %% on how the two writes happen to be coalesced.
    true = register(httpc_sse_gate, self()),
    send(S, sse_head()),
    receive
        continue -> ok
    after 5000 -> erlang:error(no_continue)
    end,
    unregister(httpc_sse_gate),
    send(S, chunk(<<"data: {\"a\":1}\n\n">>)),
    timer:sleep(600),
    send(S, chunk(<<"data: {\"b\":2}\n\n">>)),
    send(S, <<"0\r\n\r\n">>);
reply(<<"/sse-slow">>, _UA, S) ->
    %% One chunk, then a long silence before the rest.
    send(S, sse_head()),
    send(S, chunk(<<"data: {\"a\":1}\n\n">>)),
    timer:sleep(600),
    send(S, chunk(<<"data: {\"b\":2}\n\n">>)),
    send(S, <<"0\r\n\r\n">>);
reply(<<"/sse-ua">>, UA, S) ->
    send(S, sse_head()),
    send(S, chunk(<<"data: ", UA/binary, "\n\n">>)),
    send(S, <<"0\r\n\r\n">>);
reply(<<"/sse-error">>, _UA, S) ->
    send(
        S,
        <<"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 7\r\nContent-Length: 33\r\n\r\n",
            "{\"error\":{\"message\":\"slow down\"}}">>
    );
reply(<<"/v1/chat/completions">>, _UA, S) ->
    send(S, sse_head()),
    [
        begin
            send(S, chunk(C)),
            timer:sleep(5)
        end
     || C <- [
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\r\n">>,
            <<"\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"lo\"},">>,
            <<"\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]\r\n\r\n">>
        ]
    ],
    send(S, <<"0\r\n\r\n">>);
reply(<<"/close">>, _UA, S) ->
    send(S, <<"HTTP/1.1 200 OK\r\nContent-Ty">>);
reply(_, _UA, S) ->
    send(S, <<"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n">>).

sse_head() ->
    <<"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n">>.

chunk(B) -> [integer_to_list(byte_size(B), 16), "\r\n", B, "\r\n"].

send(S, Data) -> _ = ssl:send(S, Data).

read_request(S, Acc) ->
    case binary:split(Acc, <<"\r\n\r\n">>) of
        [Head, Body] ->
            Need =
                case
                    re:run(Head, "(?i)content-length: *([0-9]+)", [{capture, all_but_first, binary}])
                of
                    {match, [N]} -> binary_to_integer(N);
                    nomatch -> 0
                end,
            case byte_size(Body) < Need of
                true -> more(S, Acc);
                false -> {ok, Acc}
            end;
        [_] ->
            more(S, Acc)
    end.

more(S, Acc) ->
    case ssl:recv(S, 0, 5000) of
        {ok, D} -> read_request(S, <<Acc/binary, D/binary>>);
        E -> E
    end.

req(Ctx, Method, Path, Headers) ->
    ?T:request(#{method => Method, url => url(Ctx, Path), headers => Headers}, opts(Ctx)).

ok_response(Ctx) ->
    ?assertMatch(
        {ok, #{status := 200, body := <<"hi">>, headers := #{<<"x-dup">> := <<"1, 2">>}}},
        req(Ctx, get, <<"/ok">>, #{})
    ).

error_status(Ctx) ->
    ?assertMatch({ok, #{status := 418, body := <<"nope!">>}}, req(Ctx, get, <<"/teapot">>, #{})).

redirect(Ctx) ->
    ?assertMatch({ok, #{status := 302}}, req(Ctx, get, <<"/redirect">>, #{})).

user_agent(Ctx) ->
    ?assertMatch({ok, #{body := <<"thinwrap-llm-erlang">>}}, req(Ctx, get, <<"/echo-ua">>, #{})),
    ?assertMatch(
        {ok, #{body := <<"mine/1">>}},
        req(Ctx, get, <<"/echo-ua">>, #{<<"user-agent">> => <<"mine/1">>})
    ).

post(Ctx) ->
    flush_requests(),
    true = register(httpc_test_sink, self()),
    {ok, #{status := 200}} = ?T:request(
        #{
            method => post,
            url => url(Ctx, <<"/post">>),
            headers => #{<<"Content-Type">> => <<"application/json">>},
            body => <<"{\"a\":1}">>
        },
        opts(Ctx)
    ),
    receive
        {request, Raw} ->
            ?assertNotEqual(nomatch, binary:match(Raw, <<"{\"a\":1}">>)),
            {match, CTs} = re:run(Raw, "(?i)content-type: ([^\r]+)", [
                global, {capture, all_but_first, binary}
            ]),
            ?assertEqual([[<<"application/json">>]], CTs)
    after 5000 -> ?assert(false)
    end,
    unregister(httpc_test_sink).

hung(Ctx) ->
    ?assertEqual(
        {error, timeout},
        ?T:request(#{method => get, url => url(Ctx, <<"/hang">>), headers => #{}}, (opts(Ctx))#{
            timeout => 300
        })
    ).

refuses_non_https() ->
    [
        ?assertMatch(
            {error, {connector_error, #{code := invalid_request}}},
            ?T:request(#{method => get, url => U, headers => #{}}, #{})
        )
     || U <- [
            <<"http://localhost:8000/v1/chat/completions">>,
            <<"file:///etc/passwd">>,
            <<"ftp://h/x">>,
            <<"host-only">>
        ]
    ].

%% No `ssl` option: the OTP defaults (OS trust store + host check) must reject
%% the throwaway CA.
untrusted(Ctx) ->
    ?assertMatch(
        {error, {failed_connect, _}},
        ?T:request(#{method => get, url => url(Ctx, <<"/ok">>), headers => #{}}, #{timeout => 5000})
    ).

%% ---------------------------------------------------------------------------
%% stream/4

post_req(Ctx, Path) ->
    #{
        method => post,
        url => url(Ctx, Path),
        headers => #{<<"Content-Type">> => <<"application/json">>},
        body => <<"{}">>
    }.

record(Event, Acc) -> {cont, [Event | Acc]}.

stream_chunks(Ctx) ->
    Before = monitored_by(),
    {ok, Events} = ?T:stream(post_req(Ctx, <<"/sse">>), opts(Ctx), spy(fun record/2), []),
    [{status, 200, Headers} | Data] = lists:reverse(Events),
    ?assertEqual(<<"text/event-stream">>, maps:get(<<"content-type">>, Headers)),
    ?assert(length(Data) >= 2),
    ?assertEqual(
        <<"data: {\"a\":1}\n\ndata: [DONE]\n\n">>, iolist_to_binary([B || {data, B} <- Data])
    ),
    assert_helper_gone(Before),
    assert_mailbox_clean().

stream_error_status(Ctx) ->
    {ok, Events} = ?T:stream(post_req(Ctx, <<"/sse-error">>), opts(Ctx), fun record/2, []),
    ?assertMatch(
        [{status, 429, #{<<"retry-after">> := <<"7">>}}, {data, <<"{\"error\":", _/binary>>}],
        lists:reverse(Events)
    ),
    assert_mailbox_clean().

stream_halt(Ctx) ->
    Before = monitored_by(),
    Fun = fun
        ({status, _, _}, N) -> {cont, N};
        ({data, _}, N) -> {halt, N + 1}
    end,
    ?assertEqual({ok, 1}, ?T:stream(post_req(Ctx, <<"/sse-slow">>), opts(Ctx), spy(Fun), 0)),
    %% The helper is gone when stream/4 returns, so nothing can follow; the
    %% wait covers the rest of the body, sent after 600 ms.
    assert_helper_gone(Before),
    assert_mailbox_clean(),
    timer:sleep(800),
    assert_mailbox_clean().

stream_idle(Ctx) ->
    Before = monitored_by(),
    Fun = fun
        ({status, _, _} = E, Acc) ->
            httpc_sse_gate ! continue,
            {cont, [E | Acc]};
        (E, Acc) ->
            {cont, [E | Acc]}
    end,
    {error, timeout, Events} = ?T:stream(
        post_req(Ctx, <<"/sse-gated">>), (opts(Ctx))#{timeout => 200}, spy(Fun), []
    ),
    ?assertMatch([{data, <<"data: {\"a\":1}\n\n">>}, {status, 200, _}], Events),
    assert_helper_gone(Before),
    assert_mailbox_clean(),
    timer:sleep(800),
    assert_mailbox_clean().

stream_raise(Ctx) ->
    Before = monitored_by(),
    Fun = fun
        ({status, _, _}, A) -> {cont, A};
        ({data, _}, _) -> erlang:error(fold_bug)
    end,
    ?assertError(fold_bug, ?T:stream(post_req(Ctx, <<"/sse-slow">>), opts(Ctx), spy(Fun), ok)),
    assert_helper_gone(Before),
    assert_mailbox_clean(),
    timer:sleep(800),
    assert_mailbox_clean().

%% A response httpc cannot read (the server closes mid-headers) ends as an
%% error carrying the fold's accumulator.
stream_request_error(Ctx) ->
    ?assertMatch(
        {error, _, acc0}, ?T:stream(post_req(Ctx, <<"/close">>), opts(Ctx), fun record/2, acc0)
    ),
    assert_mailbox_clean().

%% A caller killed mid-stream must not leave its helper (or the request)
%% behind: the helper monitors the caller.
stream_caller_killed(Ctx) ->
    Self = self(),
    Before = monitored_by(),
    Caller = spawn(fun() ->
        Fun = fun
            ({status, _, _}, A) ->
                Self ! {helpers, monitored_by() -- Before},
                timer:sleep(infinity),
                {cont, A};
            (_, A) ->
                {cont, A}
        end,
        ?T:stream(post_req(Ctx, <<"/sse-slow">>), opts(Ctx), Fun, ok)
    end),
    Helpers =
        receive
            {helpers, Hs} -> Hs
        after 5000 -> erlang:error(no_status)
        end,
    ?assertMatch([_], Helpers),
    Mons = [erlang:monitor(process, H) || H <- Helpers],
    exit(Caller, kill),
    [
        receive
            {'DOWN', M, process, _, _} -> ok
        after 2000 -> erlang:error(helper_outlived_caller)
        end
     || M <- Mons
    ],
    assert_mailbox_clean().

stream_user_agent(Ctx) ->
    flush_requests(),
    true = register(httpc_test_sink, self()),
    {ok, Events} = ?T:stream(post_req(Ctx, <<"/sse-ua">>), opts(Ctx), fun record/2, []),
    ?assertEqual(
        <<"data: thinwrap-llm-erlang\n\n">>,
        iolist_to_binary([B || {data, B} <- lists:reverse(Events)])
    ),
    receive
        {request, Raw} -> ?assertNotEqual(nomatch, binary:match(Raw, <<"\r\n\r\n{}">>))
    after 5000 -> ?assert(false)
    end,
    unregister(httpc_test_sink).

stream_refuses_non_https() ->
    ?assertMatch(
        {error, {connector_error, #{code := invalid_request}}, acc0},
        ?T:stream(
            #{method => post, url => <<"http://localhost:1234/v1">>, headers => #{}, body => <<>>},
            #{},
            fun record/2,
            acc0
        )
    ).

stream_connect_failure() ->
    %% Port 1 on localhost: nothing listens there.
    ?assertMatch(
        {error, _, acc0},
        ?T:stream(
            #{method => post, url => <<"https://localhost:1/x">>, headers => #{}, body => <<>>},
            #{timeout => 2000},
            fun record/2,
            acc0
        )
    ),
    assert_mailbox_clean().

end_to_end(Ctx) ->
    Config = #{
        provider => openai,
        api_key => <<"k">>,
        base_url => url(Ctx, <<"/v1">>),
        transport => {?T, opts(Ctx)}
    },
    Input = #{model => <<"m">>, messages => [#{role => user, content => <<"hi">>}]},
    {ok, Deltas} = thinwrap_llm:stream(Config, Input, fun(D, A) -> [D | A] end, []),
    ?assertEqual(
        <<"Hello">>,
        iolist_to_binary([maps:get(content_delta, D, <<>>) || D <- lists:reverse(Deltas)])
    ),
    ?assertEqual([stop], [F || #{finish_reason := F} <- Deltas]),
    assert_mailbox_clean().

%% Nothing at all may be left: no httpc message, no helper event, no 'DOWN'.
assert_mailbox_clean() ->
    receive
        M -> ?assertEqual(no_stray_message, M)
    after 0 -> ok
    end.

%% The processes monitoring the caller; a streaming helper is one while it
%% lives.
monitored_by() ->
    {monitored_by, Pids} = process_info(self(), monitored_by),
    Pids.

%% Wraps a fold so the first event records the helper(s) that appeared since
%% `Before` was taken.
spy(Fun) ->
    fun(Event, Acc) ->
        case get(spied_helpers) of
            undefined -> put(spied_helpers, monitored_by());
            _ -> ok
        end,
        Fun(Event, Acc)
    end.

assert_helper_gone(Before) ->
    Helpers = erase(spied_helpers) -- Before,
    ?assertMatch([_], Helpers),
    ?assertEqual([], [H || H <- Helpers, is_process_alive(H)]).

flush_requests() ->
    receive
        {request, _} -> flush_requests()
    after 0 -> ok
    end.
