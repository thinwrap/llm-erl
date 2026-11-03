-module(thinwrap_llm_http).
-moduledoc false.

%% The shared HTTP seam every connector sends through: builds the request,
%% calls the configured transport (buffered or streaming), normalizes a
%% transport failure, and drives a streaming response through the incremental
%% SSE parser into the connector's per-event mapper and the caller's fold. It
%% does no status-to-code mapping: that stays in each provider module.

-export([post_json/4, post_raw/5, stream_sse/7]).

-export_type([headers/0, stream_handler/0, meta/0]).

-define(USER_EXIT, thinwrap_llm_user_exit).

-type headers() :: thinwrap_llm_transport:headers().

%% What a streaming connector plugs into `stream_sse/7`:
%% - `error`: the connector error for a non-2xx (decoded body, or `undefined`
%%   when it was empty, not JSON, or its read failed);
%% - `init`: the connector's per-stream state;
%% - `payload`: maps one decoded SSE payload to normalized deltas (may
%%   `thinwrap_llm_error:fail/1`, e.g. on a mid-stream error frame);
%% - `finish`: runs once the body ended (`SawDone` is whether `[DONE]`
%%   arrived) and returns any trailing deltas, or fails a truncated stream
%%   (`{Deltas, Error}` delivers the deltas first, then fails with `Error`).
-type stream_handler() :: #{
    error := fun((integer(), headers(), term()) -> thinwrap_llm:connector_error()),
    init := term(),
    payload := fun((term(), meta(), term()) -> {[thinwrap_llm:stream_delta()], term()}),
    finish := fun(
        (boolean(), meta(), term()) ->
            [thinwrap_llm:stream_delta()]
            | {[thinwrap_llm:stream_delta()], thinwrap_llm:connector_error()}
    )
}.

-type meta() :: #{status := integer(), headers := headers()}.

-doc false.
-spec post_json(thinwrap_llm:config(), binary(), term(), headers()) ->
    thinwrap_llm_transport:response().
post_json(Config, Url, Body, Headers) ->
    Data = thinwrap_llm_json:encode(Body),
    send(Config, #{method => post, url => Url, headers => with_json(Headers), body => Data}).

-doc false.
%% POST pre-serialized bytes (Bedrock signs the exact bytes it sends).
-spec post_raw(thinwrap_llm:config(), binary(), binary(), binary(), headers()) ->
    thinwrap_llm_transport:response().
post_raw(Config, Url, ContentType, Body, Headers) ->
    send(Config, #{
        method => post,
        url => Url,
        headers => Headers#{<<"Content-Type">> => ContentType},
        body => Body
    }).

-doc false.
%% POST `Body` as JSON with `Accept: text/event-stream` and fold the caller's
%% `UserFun` over the normalized deltas, in the calling process, as the
%% response arrives. Returns the caller's final accumulator; any failure is a
%% `thinwrap_llm_error:fail/1`.
-spec stream_sse(
    thinwrap_llm:config(),
    binary(),
    term(),
    headers(),
    stream_handler(),
    fun((thinwrap_llm:stream_delta(), Acc) -> Acc),
    Acc
) -> Acc.
stream_sse(Config, Url, Body, Headers0, Handler, UserFun, UserAcc0) ->
    Data = thinwrap_llm_json:encode(Body),
    Headers = (with_json(Headers0))#{<<"Accept">> => <<"text/event-stream">>},
    ok = check_header_values(Headers),
    Req = #{method => post, url => Url, headers => Headers, body => Data},
    Acc0 = #{
        phase => wait_status,
        meta => #{status => 0, headers => #{}},
        sse => thinwrap_llm_sse:new(),
        conn => maps:get(init, Handler),
        user => UserAcc0,
        err => []
    },
    Fold = fun(Event, Acc) -> on_event(Event, Acc, Handler, UserFun) end,
    {Mod, Opts} = maps:get(transport, Config),
    Result =
        try
            case erlang:function_exported(Mod, stream, 4) of
                true -> Mod:stream(Req, Opts, Fold, Acc0);
                false -> buffered_stream(Mod, Req, Opts, Fold, Acc0)
            end
        catch
            throw:{thinwrap_llm_error, _} = Own ->
                throw(Own);
            throw:{?USER_EXIT, Class, Reason, Stack} ->
                erlang:raise(Class, Reason, Stack);
            error:{Contract, _} = Bug when Contract =:= bad_stream_event ->
                erlang:error(Bug);
            error:{bad_transport_return, _, _} = Bug ->
                erlang:error(Bug);
            Class2:Reason2 ->
                {error, {Class2, Reason2}, Acc0}
        end,
    case Result of
        {ok, #{phase := sse} = Acc} ->
            finish_sse(Acc, Handler, UserFun);
        {ok, #{phase := error_body, meta := #{status := S, headers := H}, err := Err}} ->
            Raw = thinwrap_llm_json:decode(iolist_to_binary(lists:reverse(Err))),
            thinwrap_llm_error:fail((maps:get(error, Handler))(S, H, Raw));
        {error, {connector_error, #{code := _} = E}, _} ->
            thinwrap_llm_error:fail(E);
        {error, _Reason, #{phase := error_body, meta := #{status := S, headers := H}}} ->
            %% The provider answered; a failed read of its error body must not
            %% hide the status, which still classifies the failure.
            thinwrap_llm_error:fail((maps:get(error, Handler))(S, H, undefined));
        {error, Reason3, #{phase := _}} ->
            thinwrap_llm_error:fail(transport_failure(Reason3));
        Other ->
            erlang:error({bad_transport_return, Mod, shape(Other)})
    end.

%% A transport without stream/4: one buffered request, replayed as a status
%% event plus a single body chunk. A non-2xx never streams anyway, and a 2xx
%% body parses the same, just not token-by-token.
buffered_stream(Mod, Req, Opts, Fold, Acc0) ->
    case checked_request(Mod, Req, Opts) of
        {ok, #{status := S, headers := H, body := B}} ->
            case Fold({status, S, H}, Acc0) of
                {cont, Acc1} ->
                    {_, Acc2} = Fold({data, B}, Acc1),
                    {ok, Acc2};
                {halt, Acc1} ->
                    {ok, Acc1}
            end;
        {error, Reason} ->
            {error, Reason, Acc0}
    end.

on_event({status, S, H}, #{phase := wait_status} = Acc, _Handler, _UserFun) when
    is_integer(S), is_map(H)
->
    Meta = #{status => S, headers => H},
    case thinwrap_llm_util:ok_status(S) of
        true ->
            ok = require_event_stream(S, H),
            {cont, Acc#{phase := sse, meta := Meta}};
        false ->
            {cont, Acc#{phase := error_body, meta := Meta}}
    end;
on_event({data, Chunk}, #{phase := error_body, err := Err} = Acc, _Handler, _UserFun) when
    is_binary(Chunk)
->
    {cont, Acc#{err := [Chunk | Err]}};
on_event({data, Chunk}, #{phase := sse, sse := Sse0} = Acc, Handler, UserFun) when
    is_binary(Chunk)
->
    {Payloads, Sse1} = thinwrap_llm_sse:feed(Chunk, Sse0),
    Acc1 = payloads(Payloads, Acc#{sse := Sse1}, Handler, UserFun),
    case thinwrap_llm_sse:done(Sse1) of
        true -> {halt, Acc1};
        false -> {cont, Acc1}
    end;
on_event(Event, _Acc, _Handler, _UserFun) ->
    erlang:error({bad_stream_event, event_shape(Event)}).

finish_sse(#{sse := Sse0} = Acc0, Handler, UserFun) ->
    {Payloads, Sse1} = thinwrap_llm_sse:finish(Sse0),
    #{meta := Meta, conn := Conn, user := User} =
        payloads(Payloads, Acc0#{sse := Sse1}, Handler, UserFun),
    case (maps:get(finish, Handler))(thinwrap_llm_sse:done(Sse1), Meta, Conn) of
        %% Trailing deltas the caller must still see before the stream fails
        %% (Gemini's running usage on a truncated stream).
        {Deltas, #{code := _} = Error} ->
            _ = deliver(Deltas, UserFun, User),
            thinwrap_llm_error:fail(Error);
        Deltas ->
            deliver(Deltas, UserFun, User)
    end.

payloads([], Acc, _Handler, _UserFun) ->
    Acc;
payloads([P | Rest], #{meta := Meta, conn := Conn, user := User} = Acc, Handler, UserFun) ->
    case thinwrap_llm_json:decode(P) of
        undefined ->
            %% Not JSON (a keep-alive or a malformed frame): skipped, as the
            %% siblings do.
            payloads(Rest, Acc, Handler, UserFun);
        Json ->
            {Deltas, Conn1} = (maps:get(payload, Handler))(Json, Meta, Conn),
            payloads(
                Rest, Acc#{conn := Conn1, user := deliver(Deltas, UserFun, User)}, Handler, UserFun
            )
    end.

%% The caller's fold runs inside the transport's fold. Its exceptions are
%% tagged so they leave the library unchanged after the transport has released
%% the connection, instead of reading as a transport failure.
deliver(Deltas, UserFun, User0) ->
    lists:foldl(
        fun(D, U) ->
            try
                UserFun(D, U)
            catch
                Class:Reason:Stack -> throw({?USER_EXIT, Class, Reason, Stack})
            end
        end,
        User0,
        Deltas
    ).

%% A 2xx labelled as anything but text/event-stream (a JSON completion from a
%% proxy that dropped `stream: true`, an HTML gateway page) holds no events,
%% so reading it would end the stream with nothing and no error. A missing
%% content-type is let through; the end-of-stream check still catches an empty
%% body.
require_event_stream(Status, Headers) ->
    CT = string:trim(maps:get(<<"content-type">>, Headers, <<>>)),
    case
        CT =:= <<>> orelse binary:match(string:lowercase(CT), <<"text/event-stream">>) =/= nomatch
    of
        true ->
            ok;
        false ->
            [Media | _] = binary:split(CT, <<";">>),
            Msg = iolist_to_binary([
                <<"Provider returned a successful status with a non-SSE body (">>,
                string:trim(Media),
                <<") for a streaming request">>
            ]),
            thinwrap_llm_error:fail(
                thinwrap_llm_error:new(provider_unavailable, #{
                    status => Status, message => Msg, provider_message => Msg
                })
            )
    end.

with_json(Headers) -> maps:merge(#{<<"Content-Type">> => <<"application/json">>}, Headers).

send(Config, #{headers := Headers} = Req) ->
    ok = check_header_values(Headers),
    {Mod, Opts} = maps:get(transport, Config),
    case checked_request(Mod, Req, Opts) of
        {ok, Resp} -> Resp;
        {error, Reason} -> thinwrap_llm_error:fail(transport_failure(Reason))
    end.

checked_request(Mod, Req, Opts) ->
    Result =
        try
            Mod:request(Req, Opts)
        catch
            throw:{thinwrap_llm_error, _} = Thrown -> throw(Thrown);
            Class:Reason -> {error, {Class, Reason}}
        end,
    case Result of
        {ok, #{status := S, headers := H, body := B} = Resp} when
            is_integer(S), is_map(H), is_binary(B)
        ->
            {ok, Resp};
        {error, {connector_error, #{code := _} = E}} ->
            thinwrap_llm_error:fail(E);
        {error, Reason2} ->
            {error, Reason2};
        Other ->
            %% A transport that breaks its contract is a bug in the calling
            %% code, not a provider outage — report it as such.
            erlang:error({bad_transport_return, Mod, shape(Other)})
    end.

%% Refuse a header carrying a line break or NUL before it reaches the
%% transport. It is a configuration error (typically an API key pasted with a
%% trailing newline), not an unreachable provider, and a client's own error
%% would quote the value, which for a credential header is the secret. The
%% message names only the header.
check_header_values(Headers) ->
    maps:foreach(
        fun(Name, Value) ->
            case bad_chars(Value, "\r\n\0") orelse bad_chars(Name, "\r\n\0:") of
                true ->
                    Msg = iolist_to_binary([
                        "Header '",
                        printable(Name),
                        "' contains a line break or NUL character; check the configured "
                        "credential and headers"
                    ]),
                    thinwrap_llm_error:fail(
                        thinwrap_llm_error:new(invalid_request, #{
                            message => Msg, provider_message => <<"Invalid header value">>
                        })
                    );
                false ->
                    ok
            end
        end,
        Headers
    ).

bad_chars(Bin, Chars) when is_binary(Bin) ->
    lists:any(fun(C) -> binary:match(Bin, <<C>>) =/= nomatch end, Chars);
bad_chars(_, _) ->
    false.

printable(Name) ->
    <<
        <<
            (case C >= 32 andalso C < 127 of
                true -> <<C>>;
                false -> <<"?">>
            end)/binary
        >>
     || <<C>> <= Name
    >>.

%% Only the SHAPE of a failure is kept — never its text: a client's error may
%% embed the full request URL, which for a query-credential provider carries
%% the secret.
transport_failure(Reason) ->
    Name = reason_name(Reason),
    Msg = <<"Upstream transport error (", Name/binary, ")">>,
    thinwrap_llm_error:new(provider_unavailable, #{
        message => Msg,
        provider_message => Msg,
        cause => #{<<"raw">> => #{<<"exception">> => Name}}
    }).

reason_name({Class, Reason}) when Class =:= error; Class =:= exit; Class =:= throw ->
    reason_name(Reason);
reason_name(A) when is_atom(A) -> atom_to_binary(A);
reason_name(T) when is_tuple(T), tuple_size(T) > 0, is_atom(element(1, T)) ->
    atom_to_binary(element(1, T));
reason_name(_) ->
    <<"error">>.

shape({ok, M}) when is_map(M) -> {ok, maps:keys(M)};
shape(T) when is_tuple(T) -> {tuple, tuple_size(T)};
shape(T) when is_atom(T) -> T;
shape(_) -> other.

event_shape(T) when is_tuple(T), tuple_size(T) > 0 -> {element(1, T), tuple_size(T)};
event_shape(_) -> other.
