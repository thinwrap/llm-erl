# Perplexity (Sonar) (Erlang)

Chat via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from Perplexity is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => perplexity,
           api_key => list_to_binary(os:getenv("PERPLEXITY_API_KEY"))},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"sonar">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `perplexity`. |
| `api_key` | `binary()` or `fun(() -> binary())` | Yes | Sent as `Authorization: Bearer <api_key>`. The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | No | Overrides the default `https://api.perplexity.ai` (no `/v1` segment). |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request. |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

Pass your own transport with `transport => {Module, Opts}` (see `thinwrap_llm_transport`); the default `thinwrap_llm_httpc` is HTTPS-only and never follows redirects. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP.

## Auth setup

Create a key at <https://www.perplexity.ai/settings/api>. It is sent as `Authorization: Bearer <api_key>`.

## Chat

### Endpoint

`POST https://api.perplexity.ai/chat/completions` (the default base URL has no `/v1` prefix).

### Notes / quirks

- **No tool calling (handled):** the Sonar chat surface has no tool/function calling, so the spec sets `supports_tools => false` — `tools` and `tool_choice` on the input are **not sent** (tools live on Perplexity's separate Agent API).
- Sampling surface is stripped upstream (no `top_k` / penalties / `seed` / `n` on Sonar).
- Model ids are `sonar` / `sonar-pro` / `sonar-reasoning-pro` / `sonar-deep-research`.
- `max_output_tokens` maps to `max_tokens`. `reasoning.effort` maps to `reasoning_effort`.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process). `stream_options.include_usage` is set so the terminal delta carries `usage`.

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"sonar">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

No OpenAI-float embeddings surface — `thinwrap_llm:supports(perplexity, embed)` is `false`, and `thinwrap_llm:embed/2` raises `badarg` (misuse, not an `{error, _}`).

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Search/citation controls + sub-baseline fields go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); citations and other vendor extras in `raw`.
