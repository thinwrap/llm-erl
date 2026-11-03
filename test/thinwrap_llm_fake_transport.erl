-module(thinwrap_llm_fake_transport).
-moduledoc false.

%% Test transport implementing both request/2 and stream/4: returns queued
%% responses in order (repeating the last once exhausted) and records every
%% request. A queued item is a response map, `{error, Reason}`, or
%% `{stream_error, Resp, Reason}` (the status and body chunks are delivered,
%% then the read fails). Backed by a small linked process so the recorded calls
%% survive across the connector's call stack.
%%
%% stream/4 delivers the body in small chunks (7 bytes, or the response's
%% `chunk_size`, or its explicit `chunks` list), so every streaming test also
%% exercises lines, events and CRLFs split across chunk boundaries.

-behaviour(thinwrap_llm_transport).

-export([request/2, stream/4]).
-export([start/1, config/2, calls/1, last/1, stop/1]).
-export([resp/2, resp/3, sse/1, sse_events/1, body_json/1, path_of/1, qget/2, header/2]).

-define(CHUNK, 7).
%% The fake answers at once; a missing reply means it died, so fail loudly
%% instead of hanging the suite.
-define(REPLY_TIMEOUT, 5000).
-define(IDLE_TIMEOUT, 60000).

start(Responses) ->
    spawn_link(fun() -> loop(Responses, []) end).

%% `Config` with this fake as its transport.
config(Config, Responses) ->
    Pid = start(Responses),
    {Config#{transport => {?MODULE, Pid}}, Pid}.

request(Req, Pid) ->
    case next(Req, Pid) of
        {stream_error, _Resp, Reason} -> {error, Reason};
        {error, _} = E -> E;
        #{} = Resp -> {ok, maps:with([status, headers, body], Resp)}
    end.

stream(Req, Pid, Fun, Acc0) ->
    case next(Req, Pid) of
        {error, Reason} ->
            {error, Reason, Acc0};
        {stream_error, Resp, Reason} ->
            case deliver(Resp, Fun, Acc0) of
                {cont, Acc} -> {error, Reason, Acc};
                {halt, Acc} -> {ok, Acc}
            end;
        #{} = Resp ->
            {_, Acc} = deliver(Resp, Fun, Acc0),
            {ok, Acc}
    end.

deliver(#{status := S, headers := H} = Resp, Fun, Acc0) ->
    case Fun({status, S, H}, Acc0) of
        {cont, Acc1} -> chunks(chunks_of(Resp), Fun, Acc1);
        {halt, _} = Halt -> Halt
    end.

chunks([], _Fun, Acc) ->
    {cont, Acc};
chunks([C | Rest], Fun, Acc) ->
    case Fun({data, C}, Acc) of
        {cont, Acc1} -> chunks(Rest, Fun, Acc1);
        {halt, _} = Halt -> Halt
    end.

chunks_of(#{chunks := Cs}) -> Cs;
chunks_of(#{body := B} = Resp) -> split(B, maps:get(chunk_size, Resp, ?CHUNK)).

split(<<>>, _N) -> [];
split(B, N) when byte_size(B) =< N -> [B];
split(B, N) -> [binary:part(B, 0, N) | split(binary:part(B, N, byte_size(B) - N), N)].

next(Req, Pid) ->
    Ref = make_ref(),
    Pid ! {request, self(), Ref, Req},
    receive
        {Ref, Reply} -> Reply
    after ?REPLY_TIMEOUT -> erlang:error({fake_transport, no_reply})
    end.

calls(Pid) ->
    Ref = make_ref(),
    Pid ! {calls, self(), Ref},
    receive
        {Ref, Calls} -> Calls
    after ?REPLY_TIMEOUT -> erlang:error({fake_transport, no_reply})
    end.

last(Pid) -> lists:last(calls(Pid)).

stop(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    ok.

loop(Queue, Calls) ->
    receive
        {request, From, Ref, Req} ->
            Idx = length(Calls),
            Reply =
                case Queue of
                    [] -> resp(200, <<>>);
                    _ when Idx < length(Queue) -> lists:nth(Idx + 1, Queue);
                    _ -> lists:last(Queue)
                end,
            From ! {Ref, Reply},
            loop(Queue, Calls ++ [Req]);
        {calls, From, Ref} ->
            From ! {Ref, Calls},
            loop(Queue, Calls)
    after ?IDLE_TIMEOUT ->
        %% Each fake serves one test; one idle this long is left over from a
        %% test that never called stop/1, so it ends instead of lingering.
        ok
    end.

resp(Status, Body) -> resp(Status, Body, #{}).

resp(Status, Body, Headers) ->
    #{
        status => Status,
        headers => maps:from_list([
            {string:lowercase(thinwrap_llm_util:bin(K)), V}
         || K := V <- Headers
        ]),
        body => iolist_to_binary(Body)
    }.

%% An SSE body from raw payload texts: `data: P\n\n` each.
sse(Payloads) -> iolist_to_binary([["data: ", P, "\n\n"] || P <- Payloads]).

%% An SSE body from JSON terms.
sse_events(Events) -> sse([json:encode(E) || E <- Events]).

body_json(#{body := B}) -> json:decode(B).

path_of(#{url := Url}) -> maps:get(path, uri_string:parse(Url)).

qget(#{url := Url}, Key) ->
    Pairs =
        case uri_string:parse(Url) of
            #{query := Q} -> uri_string:dissect_query(Q);
            _ -> []
        end,
    case lists:keyfind(Key, 1, Pairs) of
        {_, V} -> V;
        false -> <<>>
    end.

%% Case-insensitive request-header lookup.
header(#{headers := H}, Name) ->
    Lower = string:lowercase(Name),
    case [V || K := V <- H, string:lowercase(K) =:= Lower] of
        [V | _] -> V;
        [] -> undefined
    end.
