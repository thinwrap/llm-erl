# OpenRouter (Erlang)

Chat + embeddings via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from OpenRouter is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => openrouter,
           api_key => list_to_binary(os:getenv("OPENROUTER_API_KEY"))},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"openai/gpt-4o">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `openrouter`. |
| `api_key` | `binary()` or `fun(() -> binary())` | Yes | Sent as `Authorization: Bearer <api_key>`. The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | No | Overrides the default `https://openrouter.ai/api/v1`. |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request — set `HTTP-Referer` / `X-Title` here for app attribution. |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

Pass your own transport with `transport => {Module, Opts}` (see `thinwrap_llm_transport`); the default `thinwrap_llm_httpc` is HTTPS-only and never follows redirects. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP.

## Auth setup

Create a key at <https://openrouter.ai/keys>. It is sent as `Authorization: Bearer <api_key>`.

## Chat

### Endpoint

`POST https://openrouter.ai/api/v1/chat/completions`

### Notes / quirks

- Aggregator: model ids are `{provider}/{model}` (e.g. `anthropic/claude-sonnet-4.5`), with optional `:free` / `:nitro` / `:floor` suffixes.
- Set `HTTP-Referer` / `X-Title` via `headers` for attribution.
- Drops params an underlying provider lacks (unless `provider.require_parameters` is set via `passthrough => #{body => ...}`); `n` is unsupported.
- `max_output_tokens` maps to `max_tokens`. `reasoning.effort` maps to `reasoning_effort`.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process). `stream_options.include_usage` is set so the terminal delta carries `usage`.

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"openai/gpt-4o">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

Supported.

```erlang
{ok, #{embeddings := Vectors, usage := #{input_tokens := In}}} =
    thinwrap_llm:embed(Config, #{model => <<"openai/text-embedding-3-small">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors :: [[float()]], one per input, in input order
```

`POST https://openrouter.ai/api/v1/embeddings` with `encoding_format: "float"`; `dimensions` maps to `dimensions` when set. Vectors are returned in input order in `embeddings`.

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- OpenRouter reports a failure after processing started as HTTP 200 with an `error` object (top-level, or on `choices[0]` with `finish_reason: "error"`); that is also an `{error, Map}`. `status` is the HTTP status the response arrived with (the 2xx); `code` comes from `error.code` (a numeric code is classified as that HTTP status, a string code such as `insufficient_quota` by its meaning), else `unknown`. The vendor's code stays in `cause` under `<<"raw">>`. The same holds for an error frame mid-stream.
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Provider-routing controls + sub-baseline fields go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); response extras in `raw`.
