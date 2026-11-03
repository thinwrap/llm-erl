# `thinwrap_llm` (Erlang) — contributor guide

This folder (`.ai/`) is for developers — and the coding agents working alongside
them — who are **changing this library**. It is not usage documentation.

> **Using the package?** See [`../README.md`](../README.md) and the per-provider
> pages under [`../docs/providers/`](../docs/providers). `.ai/` is contributor-only.

## Map of this folder

- **guidelines.md** (this file) — entry point + the two "add a provider" recipes.
- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — public module → connector → shared HTTP seam, the two tiers, the streaming design, the invariants.
- [`CONVENTIONS.md`](./CONVENTIONS.md) — data shapes, error handling, naming, testing.

## The shape in one sentence

A consumer calls `thinwrap_llm:complete(Config, Input)`,
`thinwrap_llm:stream(Config, Input, Fun, Acc0)` or `thinwrap_llm:embed(Config, Input)`;
the config's `provider` key selects a connector module, which turns the
normalized input into the vendor wire, sends it through `thinwrap_llm_http`
(the injectable transport + transport-error normalization + the streaming
driver), and turns the vendor answer back into the normalized result, stream
deltas or connector error. No global middleware, no long-lived processes (the
default `httpc` transport uses one helper per streaming call that ends with
the call).

## Two connector tiers

1. **First-class OpenAI-compatible (15).** They share ONE connector —
   `thinwrap_llm_openai_compat` (chat) and `thinwrap_llm_openai_compat_embeddings`
   (embeddings) — parameterized by a row in `thinwrap_llm_spec`. Adding one is a
   spec row, not a new module.
2. **Native adapters (`anthropic`, `bedrock`, `gemini`).** Structurally different
   wire. Each is its own `thinwrap_llm_<id>` module; Bedrock signs with
   `thinwrap_llm_sigv4` (hand-rolled AWS SigV4 on OTP `crypto`).

## Setup & verify

```bash
rebar3 compile
rebar3 fmt                    # erlfmt
rebar3 do eunit, cover        # ≥80% total line coverage
rebar3 xref
rebar3 dialyzer
rebar3 lint                   # Elvis, policy in elvis.config
rebar3 hank                   # dead code
scripts/eqwalize.sh           # eqWAlizer over src/ (see below)
rebar3 fmt --check
```

`scripts/eqwalize.sh` runs eqWAlizer through ELP and needs both on the machine:
`brew install erlang-language-platform` and Java 11+ on `PATH` (with
Homebrew's openjdk: `PATH=/opt/homebrew/opt/openjdk/bin:$PATH
scripts/eqwalize.sh`). Fix a finding with real type refinement (a guard or
pattern the code path guarantees, a narrower spec); an
`-eqwalizer({nowarn_function, F/A})` is a last resort and carries a one-line
reason.

OTP ≥ 27 (the stdlib `json` module, `-doc` attributes). **Zero runtime
dependencies — do not add any.** HTTP is OTP `httpc` behind a behaviour; JSON is
OTP `json`; SigV4 is hand-rolled on `crypto`; SSE is parsed in
`thinwrap_llm_sse`.

The Python sibling (`../llm-py/src/thinwrap/llm/`) is the reference port: same
URLs, headers, bodies, normalization, error mapping and stream-delta semantics.

## Add a first-class OpenAI-compatible provider

1. **Register the id** — add the atom to `provider()` in `thinwrap_llm.erl`
   (byte-identical to the siblings' id; quote it when it has a hyphen, as
   `'azure-openai'`).
2. **Add the spec row** — `thinwrap_llm_spec`: append to `compat_providers/0`
   and add a `get/1` clause with `default_base_url` and any quirk (`auth`,
   `supports_tools`, `max_tokens_field`, `stream_usage_path`, `strict_params`,
   `dimensions_field`). `embeddings_providers/0` is every compat provider minus
   an exclusion list: add the id there if it has **no** OpenAI-float
   `/embeddings`. A provider with no public endpoint (`<<>>`
   default) must be listed where `thinwrap_llm_input:config_keys/2` makes
   `base_url` required.
3. **Test** — add cases to `test/thinwrap_llm_core_tests.erl` (the dispatch tests
   iterate every provider automatically).
4. **Docs** — `docs/providers/<id>.md`.

## Add a native-adapter provider

Copy `thinwrap_llm_anthropic.erl`. Touch-points:

1. **Register the id** — `provider()` in `thinwrap_llm.erl`, plus `supports/2`
   and `provider_module/1` there.
2. **Config** — a `config_keys/2` clause in `thinwrap_llm_input.erl`
   (`required` / `optional`, and the check: `secret` for credentials, which may
   be a binary or a fun/0).
3. **Connector** — `src/thinwrap_llm_<id>.erl`, `-moduledoc false`, exporting
   `complete(Config, Input) -> chat_result()` and
   `stream(Config, Input, Fun, Acc0) -> Acc`. Streaming over SSE plugs a handler
   map into `thinwrap_llm_http:stream_sse/7` (see ARCHITECTURE). The vendor
   status→code mapper and response navigation are private functions in the
   same module.
4. **Tests** — `test/thinwrap_llm_<id>_tests.erl` (eunit), ported from the
   Python `tests/test_<id>.py`; use `thinwrap_llm_fake_transport:config/2`.
5. **Docs** — `docs/providers/<id>.md`.

### Definition of done (the CI gates)

```bash
rebar3 fmt && rebar3 do eunit, cover && rebar3 xref && rebar3 dialyzer && \
  rebar3 lint && rebar3 hank && scripts/eqwalize.sh && rebar3 fmt --check
```

Total coverage must stay ≥ 80%.

## Invariants you must not break

- **Zero runtime deps / no vendor SDKs.** SigV4 on `crypto`, JSON on `json`,
  HTTP on `httpc` behind the transport behaviour.
- **Stateless wrapper.** No caching, retries, conversation state, telemetry,
  and no long-lived processes, ETS tables or application env. Streaming folds
  in the caller's process (the default `httpc` transport's per-call helper
  only forwards events and ends with the call); prompt caching is a consumer concern via `passthrough`.
- **Canonical shape = OpenAI Chat Completions.** Every connector emits the same
  `chat_result()` / `stream_delta()` / `connector_error()`. That parity is the
  whole product.
- **≥90% baseline-coverage rule.** A field belongs on the normalized input or
  result only if ≥90% of providers support it normalizably; everything else
  goes to `passthrough` (input) / `raw` (output) and is never emulated. The one
  deliberate sub-90% carve-out is `reasoning.effort`.
- **Per-connector locality.** Wire translation and the status→code mapper live
  in the connector — never in `thinwrap_llm_http`.
- **Error codes** — the 7 `thinwrap_llm_error:code()` atoms, byte-identical to
  the siblings' strings. Retry hints ride in `cause` as `<<"retryAfter">>` and
  `<<"retryAfterSeconds">>`; there is no top-level retry key.
- **Two kinds of failure** — operation failures are `{error, connector_error()}`;
  misuse (including a config of the wrong shape for the provider, or a provider
  lacking the operation) raises `{badarg, Description}`. See CONVENTIONS.
