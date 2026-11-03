# OpenAI (Erlang)

Chat + embeddings via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from OpenAI is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => openai,
           api_key => list_to_binary(os:getenv("OPENAI_API_KEY"))},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"gpt-4o">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `openai`. |
| `api_key` | `binary()` or `fun(() -> binary())` | Yes | Sent as `Authorization: Bearer <api_key>`. The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | No | Overrides the default `https://api.openai.com/v1`. |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request. |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

Pass your own transport with `transport => {Module, Opts}` (see `thinwrap_llm_transport`); the default `thinwrap_llm_httpc` is HTTPS-only and never follows redirects. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP.

## Auth setup

Create an API key at <https://platform.openai.com/api-keys>. It is sent verbatim as `Authorization: Bearer <api_key>`.

## Chat

### Endpoint

`POST https://api.openai.com/v1/chat/completions`

### Notes / quirks

- The reference provider — its Chat Completions shape is the canonical facade shape.
- `max_output_tokens` maps to `max_completion_tokens` (OpenAI deprecates `max_tokens`, which o-series models reject; the cap includes reasoning tokens). `reasoning.effort` maps to `reasoning_effort`.
- Reasoning models (o-series / GPT-5-class) reject `temperature`/`top_p`/penalties with HTTP 400; guard by model family and leave those normalized sampling fields unset.
- OpenAI also ships the newer Responses API; this connector targets Chat Completions.
- **Function tools together with reasoning:** current models (gpt-5.4, gpt-5.4-mini, gpt-5.5, gpt-5.6-*) reject function tools combined with reasoning on Chat Completions with a 400, and gpt-5.6 does so even when `reasoning` is unset. The workaround is `passthrough => #{body => #{reasoning_effort => <<"none">>}}`, accepted on gpt-5.4 and later (gpt-5-mini and o4-mini reject `"none"`). Moving this connector to the Responses API is planned.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process). `stream_options.include_usage` is set so the terminal delta carries `usage`.

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"gpt-4o">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

Supported.

```erlang
{ok, #{embeddings := Vectors, usage := #{input_tokens := In}}} =
    thinwrap_llm:embed(Config, #{model => <<"text-embedding-3-small">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors :: [[float()]], one per input, in input order
```

`POST https://api.openai.com/v1/embeddings` with `encoding_format: "float"`. `dimensions` maps to `dimensions` (Matryoshka) when set. Vectors are returned in input order in `embeddings`.

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Sub-baseline request fields (`seed`, `frequency_penalty`/`presence_penalty`, `logit_bias`, `n`, prompt caching) go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); response extras in `raw`.
