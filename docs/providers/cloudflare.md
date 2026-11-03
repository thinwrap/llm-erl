# Cloudflare Workers AI (Erlang)

Chat + embeddings via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from Cloudflare Workers AI is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => cloudflare,
           api_key => list_to_binary(os:getenv("CLOUDFLARE_API_TOKEN")),
           base_url => <<"https://api.cloudflare.com/client/v4/accounts/<accountId>/ai/v1">>},  % required
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"@cf/meta/llama-3.1-8b-instruct">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `cloudflare`. |
| `api_key` | `binary()` or `fun(() -> binary())` | Yes | Sent as `Authorization: Bearer <api_key>`. The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | **Yes** | No public default — a config without it raises `badarg` (it embeds your account id). |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request. |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

Pass your own transport with `transport => {Module, Opts}` (see `thinwrap_llm_transport`); the default `thinwrap_llm_httpc` is HTTPS-only and never follows redirects. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP.

## Auth setup

Create an API token (Workers AI scope) in the Cloudflare dashboard. It is sent as `Authorization: Bearer <api_key>`.

## Chat

### Endpoint

`POST <base_url>/chat/completions` — `base_url` is `https://api.cloudflare.com/client/v4/accounts/<accountId>/ai/v1`. Omitting `base_url` raises `badarg`.

### Notes / quirks

- **`base_url` is required** (no fixed public default — it embeds your account id).
- Model ids are `@cf/{provider}/{model}` (some `@hf/…`), required verbatim.
- JSON mode is **not strict** (no schema-adherence guarantee) and can't stream.
- `max_output_tokens` maps to `max_tokens`. `reasoning.effort` maps to `reasoning_effort`.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process). `stream_options.include_usage` is set so the terminal delta carries `usage`.

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"@cf/meta/llama-3.1-8b-instruct">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

Supported.

```erlang
{ok, #{embeddings := Vectors, usage := #{input_tokens := In}}} =
    thinwrap_llm:embed(Config, #{model => <<"@cf/baai/bge-large-en-v1.5">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors :: [[float()]], one per input, in input order
```

`POST <base_url>/embeddings` with `encoding_format: "float"`; `dimensions` maps to `dimensions` when set. Vectors are returned in input order in `embeddings`.

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Sub-baseline request fields go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); response extras in `raw`.
