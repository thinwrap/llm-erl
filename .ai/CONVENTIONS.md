# Conventions — `thinwrap_llm` (Erlang)

## Files and names

- Every module is prefixed `thinwrap_llm_` (Erlang has one global module
  namespace). Connectors: `thinwrap_llm_openai_compat`,
  `thinwrap_llm_openai_compat_embeddings`, `thinwrap_llm_<native>`.
- Shared concerns, one module each: `thinwrap_llm_transport` (behaviour),
  `thinwrap_llm_httpc` (default transport), `thinwrap_llm_http` (seam +
  streaming driver), `thinwrap_llm_sse` (incremental SSE), `thinwrap_llm_error`
  (codes, builders, Retry-After), `thinwrap_llm_json` (encode/decode +
  absence-tolerant accessors), `thinwrap_llm_util`, `thinwrap_llm_passthrough`,
  `thinwrap_llm_input` (validation), `thinwrap_llm_spec` (compat rows),
  `thinwrap_llm_sigv4`.
- Internal modules carry `-moduledoc false.`; internal exported functions
  carry `-doc false.`. Every exported function has a `-spec`
  (`warn_missing_spec` is on, `warnings_as_errors` too).
- Formatting is erlfmt (`rebar3 fmt`); CI runs `rebar3 fmt --check`.
- Lint is Elvis (`rebar3 lint`, policy and its few deviations in
  `elvis.config`), dead code is hank (`rebar3 hank`), and gradual typing is
  eqWAlizer over `src/` (`scripts/eqwalize.sh`; needs ELP —
  `brew install erlang-language-platform` — and Java 11+ on `PATH`). All three
  gate CI. Call other modules by their literal name: no `-import`, no module
  alias macros (Elvis reads unexpanded source, so `?M:f()` reads as a dynamic
  call).
- Comments explain WHY (vendor quirks, live-verified behaviour, billing), not
  the history of the change.

## Data shapes

| Thing | Shape |
|---|---|
| normalized input/output | map, **atom** keys (`max_output_tokens`, `tool_call_delta`) |
| strings | binaries, UTF-8 |
| closed vocabularies | atoms, byte-identical to the sibling string values (`'azure-openai'`, `tool_calls`, `context_length_exceeded`); roles `system \| user \| assistant \| tool` |
| `reasoning.effort` | `low \| medium \| high` or a binary sent as given (normalized to a binary after validation) |
| absent optional output | key **omitted** (`thinwrap_llm_util:put_opt/3`, which also drops `<<>>`) — never `undefined`/`null`/`nil` |
| stream delta | every key optional; empty strings omitted; `raw` is the vendor event |
| usage counts | integers |
| vendor JSON (request bodies, `raw`, `cause`) | maps with **binary** keys; decoded JSON `null` is the atom `null` |
| `provider_metadata` | binary-keyed by provider id (`<<"gemini">>`, `<<"anthropic">>`); opaque |
| passthrough | `#{body => map(), headers => map(), query => map()}`; keys atoms or binaries, sent as text |
| tool arguments | the JSON text, a binary |

Validated inputs come back from `thinwrap_llm_input` with the siblings'
defaults filled in (`content => <<>>`, `tools => []`, `stop => []`,
`tool_call_id => <<>>`, …), so connectors pattern-match without `maps:get/3`
fallbacks.

Python's dicts keep insertion order; Erlang maps do not. JSON bodies, the
passthrough query (sorted by key) and re-encoded tool arguments therefore can
differ from the siblings in key ORDER only, never in content.

## Two kinds of failure

- **Operation failure** → `{error, connector_error()}`: vendor error, transport
  failure, request *data* the vendor would reject (`invalid_request`: an
  unencodable body, a header with a line break, a Bedrock `region` that is not a
  region name), a 2xx that is not a result (`provider_unavailable`). Inside a
  connector: `thinwrap_llm_error:fail(thinwrap_llm_error:invalid_request(Msg))`
  and friends (`vendor_error/5`, `empty_body/1`, `malformed_body/3`,
  `stream_ended_early/0`).
- **Misuse** → `erlang:error({badarg, Description})`, raised by
  `thinwrap_llm_input` / `thinwrap_llm` before any HTTP: unknown/missing keys
  (including another provider's config keys, and a missing `base_url` for
  `'azure-openai'`/`cloudflare`), wrong types, enum atom outside its set, a
  provider lacking the operation (`embed` on `anthropic`), an unknown provider.
  Descriptions never include config or input values (credentials, prompts).

Where the Python sibling raises `ConnectorError`, Erlang returns `{error, _}`;
where it raises `TypeError`/`ValueError`, or the misuse is one of the above,
Erlang raises `badarg`.

## Error mapping

Each connector has a private status→code mapper porting the sibling truth
table:

| HTTP | code |
|---|---|
| 401 / 403 | `auth_failed` |
| 402 / 429 | `rate_limited` |
| 408 / 5xx | `provider_unavailable` |
| other 4xx | `invalid_request` (or `context_length_exceeded` / `content_filtered` on the vendor's specific signal) |
| transport | `provider_unavailable` (no `status`) |

Build the non-2xx error with `thinwrap_llm_error:vendor_error(Status, Headers,
RawBody, Code, ProviderMessage)` — it builds `cause` (`<<"raw">>`, plus
`<<"retryAfter">>` and `<<"retryAfterSeconds">>` from a usable `Retry-After`)
and falls back to `HTTP <status>` for an empty message. An error inside a 2xx
body or a stream frame keeps the status it arrived with and is classified by
its own code/type (`unknown` without one).

## Credentials

Read them with `thinwrap_llm_input:secret(Config, Key)` at the point of use (a
credential may be a fun/0). Never put a credential in an error message or cause.

## Testing

- eunit, one `test/thinwrap_llm_<area>_tests.erl` per Python test file
  (`core` also carries `test_dispatch_all.py`).
- `thinwrap_llm_fake_transport:config(Config, Responses)` returns
  `{Config1, Pid}`. Queued items: response maps (`resp/2,3`), `{error, Reason}`,
  or `{stream_error, Resp, Reason}` (status and body delivered, then the read
  fails). It implements `stream/4` and splits every streamed body into 7-byte
  chunks (or a response's `chunk_size` / explicit `chunks`), so every streaming
  test also exercises lines, events and CRLFs split across chunk boundaries.
  Helpers: `calls/1`, `last/1`, `sse/1` (payload texts → SSE body),
  `sse_events/1` (JSON terms → SSE body), `body_json/1`, `path_of/1`, `qget/2`,
  `header/2`.
- Streaming assertions: fold into a list, or send each delta to `self()` to
  check what was delivered before an error ended the stream.
- Assert the captured request (URL, headers, body), the normalized result, the
  deltas, and the error `code`/`status`.
- `thinwrap_llm_httpc_tests` exercises the real adapter (buffered and streaming)
  against a local TLS listener; no test touches the network. BEAM floats are
  always finite, so the siblings' NaN/Infinity tests have no port.
