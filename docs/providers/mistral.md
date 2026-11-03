# Mistral (Erlang)

Chat + embeddings via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from Mistral is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => mistral,
           api_key => list_to_binary(os:getenv("MISTRAL_API_KEY"))},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"mistral-large-latest">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `mistral`. |
| `api_key` | `binary()` or `fun(() -> binary())` | Yes | Sent as `Authorization: Bearer <api_key>`. The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | No | Overrides the default `https://api.mistral.ai/v1`. |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request. |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

Pass your own transport with `transport => {Module, Opts}` (see `thinwrap_llm_transport`); the default `thinwrap_llm_httpc` is HTTPS-only and never follows redirects. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP.

## Auth setup

Create a key at <https://console.mistral.ai/api-keys>. It is sent as `Authorization: Bearer <api_key>`.

## Chat

### Endpoint

`POST https://api.mistral.ai/v1/chat/completions`

### Notes / quirks

- **Strict-params quirk (handled):** Mistral 422s on unknown params, so the connector omits OpenAI-only extras — notably `stream_options` is **not** sent on streaming requests (spec `strict_params => true`). Keep `passthrough => #{body => ...}` to known Mistral fields.
- Because `stream_options.include_usage` is not sent, streaming `usage` is only present if Mistral includes it natively on the final chunk.
- Model ids are date-stamped (e.g. `-YYMM`).
- `max_output_tokens` maps to `max_tokens`. `reasoning.effort` maps to `reasoning_effort`.
- With reasoning, Mistral returns `content` as a list of `thinking` + `text` chunks; the result's `message.content` / a delta's `content_delta` carry only the `text` chunks, and the reasoning stays on `raw`.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process).

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"mistral-large-latest">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

Supported.

```erlang
{ok, #{embeddings := Vectors, usage := #{input_tokens := In}}} =
    thinwrap_llm:embed(Config, #{model => <<"mistral-embed">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors :: [[float()]], one per input, in input order
```

`POST https://api.mistral.ai/v1/embeddings` with `encoding_format: "float"`; `dimensions` maps to Mistral's `output_dimension` when set (models that support a configurable size, e.g. `codestral-embed`). Vectors are returned in input order in `embeddings`.

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Sub-baseline request fields go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); response extras in `raw`.
