# LM Studio (self-host) (Erlang)

Chat + embeddings via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from LM Studio is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => lmstudio,
           %% the http:// default needs your own transport (see Configuration)
           transport => {my_http_transport, #{}}},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"<loaded-model>">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `lmstudio`. |
| `api_key` | `binary()` or `fun(() -> binary())` | No | Optional — sent as `Authorization: Bearer <api_key>` only if set (API-token auth since v0.4.0; off by default). The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | No* | Overrides the default `http://localhost:1234/v1`. *Required if not on `:1234`. |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request. |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

The default transport (`thinwrap_llm_httpc`) is **HTTPS-only** (and never follows redirects), so it refuses the `http://` default with `{error, #{code := invalid_request}}` that says so. For a cleartext local server, pass your own transport: `Config#{transport => {my_transport, Opts}}`, where `my_transport` implements the `thinwrap_llm_transport` behaviour (`request/2`, and optionally `stream/4` for incremental streaming); or serve it behind HTTPS and set `base_url`. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc.

## Auth setup

Local single-user server; auth is off by default (leave `api_key` out → no auth header). If you enabled API-token auth, pass the token as `api_key`; it is sent as `Authorization: Bearer <api_key>`.

## Chat

### Endpoint

`POST http://localhost:1234/v1/chat/completions` (override `base_url` if not on `:1234`).

### Notes / quirks

- Self-host: with a single model loaded, the server ignores the requested `model` id and serves whatever is loaded.
- Structured output is grammar-enforced (llama.cpp / MLX); small models can be unreliable.
- `max_output_tokens` maps to `max_tokens`. `reasoning.effort` maps to `reasoning_effort`.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process). `stream_options.include_usage` is set so the terminal delta carries `usage`.

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"<loaded-model>">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

Supported (an embedding model must be loaded).

```erlang
{ok, #{embeddings := Vectors, usage := #{input_tokens := In}}} =
    thinwrap_llm:embed(Config, #{model => <<"<loaded-embedding-model>">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors :: [[float()]], one per input, in input order
```

`POST http://localhost:1234/v1/embeddings` with `encoding_format: "float"`; `dimensions` maps to `dimensions` when set. Vectors are returned in input order in `embeddings`.

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Sub-baseline request fields go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); response extras in `raw`.
