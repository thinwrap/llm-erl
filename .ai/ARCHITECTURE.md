# Architecture — `thinwrap_llm` (Erlang)

## Public module → connector → HTTP seam

```
thinwrap_llm:complete(Config, Input)        thinwrap_llm:stream(Config, Input, Fun, Acc0)
   │  thinwrap_llm_input:config/1             badarg on misuse (keys, types, transport)
   │  supports(Provider, Op)                  badarg if the provider lacks the op
   │  thinwrap_llm_input:chat_input/1         badarg on misuse; fills defaults
   ▼
thinwrap_llm_openai_compat | thinwrap_llm_anthropic | thinwrap_llm_bedrock | thinwrap_llm_gemini
   │  builds URL/headers/body, merges passthrough
   ▼
thinwrap_llm_http:post_json/4 | post_raw/5 | stream_sse/7     (shared seam)
   │  {Mod, Opts} = transport;  Mod:request/2  or  Mod:stream/4
   ▼
thinwrap_llm_httpc (default) | caller's module                 (transport behaviour)
```

`thinwrap_llm:embed/2` takes the same path into
`thinwrap_llm_openai_compat_embeddings`.

- **One public module.** `thinwrap_llm` exports `complete/2`, `stream/4`,
  `embed/2` and `supports/2`. It validates, dispatches, and converts a thrown
  connector error into `{error, Error}`. Nothing else is called by consumers
  except the `thinwrap_llm_transport` behaviour and the `thinwrap_llm_httpc`
  adapter (as a transport).
- **Config-as-selector.** `#{provider => openai, api_key => K}`; the provider
  atom picks the connector (the 15 OpenAI-compatible ids all map to
  `thinwrap_llm_openai_compat`, which reads its row from `thinwrap_llm_spec`).
  The Python sibling takes the provider id and a typed config class separately;
  here both live in one map, so "wrong config class" becomes an unknown or
  missing key — misuse, `badarg`.
- **`thinwrap_llm_http`** centralizes only the HTTP seam: request assembly, the
  header-value check (a CR/LF/NUL in a header is `invalid_request` naming only
  the header), the transport call, transport-failure normalization, and the
  streaming driver. A transport failure becomes `provider_unavailable` carrying
  only the SHAPE of the reason (`Upstream transport error (failed_connect)`,
  cause `#{<<"raw">> => #{<<"exception">> => <<"failed_connect">>}}`), never its
  text: a client's error may embed the request URL and its credentials. A
  crashing transport is treated the same way; a transport that returns a
  malformed value raises `{bad_transport_return, Mod, Shape}` (caller bug, not an
  outage). No status-to-code mapping.

## Non-local failure

Connector code fails with `thinwrap_llm_error:fail(Error)`, which throws
`{thinwrap_llm_error, Error}`. Only `thinwrap_llm:run/1` catches it. This keeps
connector code linear — the Python sibling raises `ConnectorError` the same
way — while the public surface stays `{ok, _} | {error, _}`.

## Two connector tiers

1. **OpenAI-compatible** — `thinwrap_llm_openai_compat` (chat) and
   `thinwrap_llm_openai_compat_embeddings`, parameterized by `thinwrap_llm_spec`
   rows: base URL, Bearer vs Azure `api-key` auth, `supports_tools` (Perplexity),
   `max_tokens_field` (`max_completion_tokens` for OpenAI/Azure),
   `stream_usage_path` (Groq's `x_groq.usage`), `strict_params` (Mistral: no
   `stream_options`), `dimensions_field` (Mistral `output_dimension`).
2. **Natives** — `thinwrap_llm_anthropic` (Messages API), `thinwrap_llm_bedrock`
   (Converse, SigV4 via `thinwrap_llm_sigv4` on `crypto`), `thinwrap_llm_gemini`
   (`generateContent` / `streamGenerateContent?alt=sse`). Each is a full wire
   translation emitting the identical normalized shapes.

## Streaming design

`thinwrap_llm:stream(Config, Input, Fun, Acc0)` is a **callback fold in the
caller's process**: `Fun(Delta, Acc) -> Acc` runs once per normalized delta as
the bytes arrive, and the call returns `{ok, AccFinal}` or `{error, E}`. The
library owns no long-lived processes — the decision was a fold rather than a
library-spawned producer, so there is nothing to supervise, link or leak. The
one exception is internal to the default transport: `thinwrap_llm_httpc`
runs each streaming request in a helper process that the caller spawns and
monitors, which forwards httpc's messages and is waited for (its `'DOWN'`)
before `stream/4` returns, so the caller's mailbox is provably clean. A
transport of your own is unaffected.

### Transport behaviour

`thinwrap_llm_transport` has a required `request/2` (buffered) and an optional
`stream/4`:

```erlang
-callback stream(Request, Opts, Fun, Acc0) -> {ok, Acc} | {error, Reason, Acc}.
%% Fun(Event, Acc) -> {cont, Acc1} | {halt, Acc1}
%% Event = {status, Status, Headers}   (exactly once, first; headers lower-cased)
%%       | {data, Chunk}               (raw body bytes, split anywhere)
```

- `{halt, Acc}` means the provider's terminal event arrived (`[DONE]`); the
  transport stops reading and releases the connection.
- `{error, Reason, AccSoFar}` returns the fold state so the driver can still
  classify a non-2xx whose body read broke off (by its status, as the siblings
  do when the error-body read fails).
- `Fun` may raise; the transport must release the connection and let it
  propagate.
- A non-2xx arrives as a status event plus its body, never as an error.
- **Buffered fallback:** a transport without `stream/4` still streams — the
  driver sends the request through `request/2` and replays the response as one
  status event plus one data chunk. Functionally identical, just not
  token-by-token.

### Default adapter (`thinwrap_llm_httpc`)

- `stream/4` runs `httpc` with `{sync, false}, {stream, self}` inside a
  per-call helper process (spawned and monitored by the caller), which
  forwards each event to the caller as `{Ref, Event}`; the fold runs in the
  caller.
- `httpc` streams only a 200/206 body; any other status (including a 3xx, since
  redirects are never followed) arrives as one complete response message, which
  is delivered as the status event plus one data chunk and mapped through the
  connector's error mapping.
- `httpc`'s own `timeout` would bound the whole exchange and cut a long answer
  off, so streaming passes `infinity` there and enforces the `timeout` option
  as an **idle budget per chunk** in the receive loop.
- On every exit path — end of body, halt, idle timeout, failure, or an
  exception raised by the fold — the caller tells the helper to stop (it runs
  `httpc:cancel_request/2` and exits) and drains `{Ref, _}` until the helper's
  `'DOWN'`. `cancel_request/2` is asynchronous, so a cancel-then-flush in the
  caller could never be race-free; pairwise message ordering makes the
  `'DOWN'` the helper's last word, so nothing stray can stay in the caller's
  mailbox. The helper monitors the caller and cancels if the caller dies.
- Body bytes that arrive in the same socket read as the response headers are
  held by `httpc` until the next read (OTP limitation, both stream modes; see
  the `thinwrap_llm_httpc` module doc for the source locations).
- Default `timeout` is 600 000 ms: the whole-request budget for `request/2`
  (a non-streaming reasoning call sends nothing until the model is done; 600 s
  matches the official OpenAI and Anthropic SDK defaults) and the idle budget
  between chunks for `stream/4`.
- HTTPS-only (an `http://` URL — the vLLM/Ollama/LM Studio defaults — is an
  `invalid_request` telling the caller to bring a transport), never follows
  redirects, binary bodies, `User-Agent: thinwrap-llm-erlang` (Cloudflare-fronted
  vendors such as Groq 403 a missing or library-default UA), OTP's verified-TLS
  defaults unless a complete `ssl` list is passed.

### Incremental SSE parser (`thinwrap_llm_sse`)

`new/0`, `feed(Chunk, S) -> {Payloads, S}`, `finish(S) -> {Payloads, S}`,
`done(S)`. It keeps the unterminated tail between chunks:

- line terminators are CR, LF and CRLF only (never U+2028 etc., which may sit
  inside a JSON string); a CR as the last byte of a chunk is remembered so an LF
  at the start of the next chunk completes that CRLF instead of producing a
  phantom blank line;
- `data:` strips exactly one optional leading space; multi-line data joins with
  `\n`; comments and other fields are skipped;
- `[DONE]` sets `done` and stops consuming (the driver halts the transport);
- at end of body the tail is a final line and a pending event is dispatched
  even without its blank line;
- a single line or the cumulative data of one event over 16 MiB is
  `provider_unavailable`, so an adversarial upstream cannot grow memory without
  bound.

### Driver (`thinwrap_llm_http:stream_sse/7`)

A streaming connector supplies a handler map:

```erlang
#{error   := fun((Status, Headers, RawOrUndefined) -> connector_error()),
  init    := State0,
  payload := fun((DecodedJson, #{status, headers}, State) -> {[Delta], State}),
  finish  := fun((SawDone, #{status, headers}, State) -> [Delta])}
```

The driver sends the JSON body with `Accept: text/event-stream`, then:

- non-2xx status → collect the body, decode it, `fail(error(...))`;
- 2xx with a content-type that is present and not `text/event-stream` →
  `provider_unavailable` with the 2xx status and the media type in the message
  (a proxy that dropped `stream: true`, an HTML gateway page);
- 2xx SSE → feed chunks through the parser; each JSON payload goes to
  `payload` (non-JSON payloads are skipped), and each returned delta to the
  caller's `Fun`;
- end of body → `finish` (e.g. Gemini's terminal delta), which fails a stream
  that ended before the provider's terminal event with `provider_unavailable`
  ("Stream ended before the provider sent its terminal event") rather than
  presenting a truncated answer as complete;
- a mid-stream error frame is a `fail/1` from `payload`, carrying the status the
  stream arrived with and a code from the frame's own type/code.

The caller's `Fun` runs inside the transport's fold, so its exceptions are
tagged and re-raised unchanged (same class, reason and stacktrace) after the
transport has cleaned up — never mistaken for a transport failure.

### Bedrock

Bedrock's `converse-stream` uses AWS's binary event-stream framing
(`application/vnd.amazon.eventstream`), not SSE. As in the Python sibling,
`thinwrap_llm_bedrock:stream/4` is a **non-incremental fallback**: one
`/converse` call, then the whole result folded as deltas (an optional content
delta, one `tool_call_delta` per tool call, a terminal delta with
`finish_reason` + `usage` + `raw`). A binary event-stream parser is a documented
follow-up.

## LLM-distinctive behaviour

Same as the siblings; see `../llm-py/.ai/ARCHITECTURE.md`. In short: OpenRouter
2xx in-body errors keep their 2xx status and classify by `error.code`/`type`;
Anthropic adaptive thinking (`thinking: {type: adaptive}` + `output_config.effort`,
never `budget_tokens`) with temperature/top_p omitted while reasoning;
`provider_metadata` round-trips (Gemini `thoughtSignature` per tool call, with
the `skip_thought_signature_validator` bypass on the first call of an unsigned
turn; Anthropic verbatim content blocks, replayed only while they still match
the message, and every later assistant turn rebuilt once one is); usage counts
cache and thinking tokens.

## Erlang-specific choices

- **Synchronous calls** in the caller's process; streaming is a fold.
- **Maps everywhere**, atom keys for normalized data, binary keys for vendor
  JSON (`raw`, request bodies, `provider_metadata`, `cause`). No records in the
  public API — they are awkward from Elixir and Gleam.
- **BEAM floats are always finite**, so the siblings' NaN/Infinity checks
  (embedding cells) have no Erlang equivalent; a non-number is still rejected.
- **No duck-typed recovery of raised HTTP errors.** The Python sibling rebuilds
  a response from a BYO transport that raises on non-2xx; the Erlang transport
  contract returns non-2xx as `{ok, Response}`, so there is nothing to recover.
