-module(thinwrap_llm_transport).
-moduledoc """
The bring-your-own HTTP seam.

The library never depends on a third-party HTTP client. It defines this
behaviour and ships `thinwrap_llm_httpc` (OTP `httpc`) as the default. To use
`hackney`, `gun`, `Finch`, … implement the behaviour and pass it in the
config:

```erlang
Config = #{provider => openai, api_key => Key,
           transport => {my_hackney_transport, #{pool => llm}}}.
```

## `request/2` (required)

Sends one request and returns one buffered response. Rules:

- A non-2xx answer is a **response**, never an error: return
  `{ok, #{status := 429, ...}}`. The connector owns status-to-error mapping.
- Any `{error, Reason}` is reported as `provider_unavailable`. Only the shape
  of the reason (its atom, or the atom tag of a tuple) is kept; the reason is
  never returned or printed, because a client's error may embed the request
  URL and its credentials.
- `{error, {connector_error, Error}}` surfaces a ready-made
  `t:thinwrap_llm:connector_error/0` unchanged (the default transport uses it
  to refuse non-`https` URLs).
- Never follow redirects: a 3xx must come back as a response, or the
  provider's credentials would be re-sent to wherever it points.

## `stream/4` (optional)

Sends one request and delivers the response incrementally, **in the calling
process**, by folding `Fun` over these events:

1. `{status, Status, Headers}` — exactly once, first (headers lower-cased);
2. `{data, Chunk}` — zero or more raw body chunks, split anywhere.

`Fun(Event, Acc)` returns `{cont, Acc1}` to keep reading or `{halt, Acc1}`
to stop (the provider's terminal event arrived); stop reading and release the
connection either way. Return `{ok, AccFinal}` once the body ended or the fold
halted, and `{error, Reason, AccSoFar}` when the request or a read failed
(the accumulator lets the connector still classify a non-2xx whose body read
broke off). `Fun` may raise; release the connection and let it propagate. The
non-2xx and redirect rules above apply unchanged: a non-2xx arrives as a status
event plus its body.

A transport without `stream/4` still streams: the connector sends the request
through `request/2` and parses the buffered body, which is functionally the
same but not token-by-token.
""".

-export_type([
    request/0,
    response/0,
    headers/0,
    method/0,
    transport/0,
    stream_event/0,
    stream_fun/0
]).

-type method() :: get | post.
%% Header names as given by the connector (request) or lower-cased (response).
-type headers() :: #{binary() => binary()}.

-type request() :: #{
    method := method(),
    url := binary(),
    headers := headers(),
    body => binary()
}.

%% `headers` keys MUST be lower-cased; repeated headers joined with `", "`.
-type response() :: #{
    status := 100..599,
    headers := headers(),
    body := binary()
}.

-type transport() :: {module(), Opts :: term()}.

-type stream_event() :: {status, 100..599, headers()} | {data, binary()}.

-type stream_fun() :: fun((stream_event(), Acc :: term()) -> {cont, term()} | {halt, term()}).

-callback request(Request :: request(), Opts :: term()) ->
    {ok, response()} | {error, term()}.

-callback stream(Request :: request(), Opts :: term(), Fun :: stream_fun(), Acc0 :: term()) ->
    {ok, term()} | {error, term(), term()}.

-optional_callbacks([stream/4]).
