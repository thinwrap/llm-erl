# Azure OpenAI (Erlang)

Chat + embeddings via the shared OpenAI-compatible connector (`thinwrap_llm_openai_compat`, parameterized by a row in `thinwrap_llm_spec` — not a per-provider module). Switching to/from Azure OpenAI is just the config's `provider` + the input's `model`; the input and result maps are identical to every other provider.

## Quick start

```erlang
Config = #{provider => 'azure-openai',
           api_key => list_to_binary(os:getenv("AZURE_OPENAI_API_KEY")),
           base_url => <<"https://<resource>.openai.azure.com/openai/v1">>},  % required
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"<your-deployment-name>">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | Yes | `'azure-openai'`. |
| `api_key` | `binary()` or `fun(() -> binary())` | Yes | Sent as the `api-key: <api_key>` header (not Bearer). The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | **Yes** | No public default — a config without it raises `badarg` (misuse, before any HTTP). |
| `headers` | `#{binary() => binary()}` | No | Merged onto every request (put an Entra bearer token here for OAuth mode). |
| `transport` | `{module(), Opts}` | No | Default `{thinwrap_llm_httpc, #{}}`. |

Pass your own transport with `transport => {Module, Opts}` (see `thinwrap_llm_transport`); the default `thinwrap_llm_httpc` is HTTPS-only and never follows redirects. When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc. An unknown or missing config key (e.g. an Anthropic-only key on this provider) raises `badarg` before any HTTP.

## Auth setup

Get the key + resource endpoint from the Azure portal. The connector sends it as the `api-key` header. Microsoft Entra OAuth is also accepted on the v1 GA API — supply that bearer token via `headers` instead.

## Chat

### Endpoint

`POST <base_url>/chat/completions` — `base_url` is `https://<resource>.openai.azure.com/openai/v1` (v1 GA API, opt-in from Aug 2025; no `api-version` query needed). Omitting `base_url` raises `badarg`.

### Notes / quirks

- **`base_url` is required** (no fixed public default).
- `model` is the per-resource **deployment name**, not a portable global model id.
- `max_output_tokens` maps to `max_completion_tokens` (Azure reasoning models accept only that on Chat Completions; the cap includes reasoning tokens). `reasoning.effort` maps to `reasoning_effort`.
- `max_completion_tokens` needs the v1 API (`/openai/v1`, no `api-version`) or a dated `api-version` of `2024-09-01-preview` or later; Microsoft's API changelog adds it in `2024-09-01-preview`, so older dated versions predate it. A dated version goes in `passthrough => #{query => #{<<"api-version">> => ...}}`.
- **Function tools together with reasoning:** current models (gpt-5.4, gpt-5.4-mini, gpt-5.5, gpt-5.6-*) reject function tools combined with reasoning on Chat Completions with a 400, and gpt-5.6 does so even when `reasoning` is unset. The workaround is `passthrough => #{body => #{reasoning_effort => <<"none">>}}`, accepted on gpt-5.4 and later (gpt-5-mini and o4-mini reject `"none"`). Moving this connector to the Responses API is planned.
- Streaming via `thinwrap_llm:stream(Config, Input, fun(Delta, Acc) -> ... end, Acc0)` (a fold in the calling process). `stream_options.include_usage` is set so the terminal delta carries `usage`.

```erlang
{ok, _} = thinwrap_llm:stream(Config, #{model => <<"<your-deployment-name>">>, messages => Msgs},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

## Embeddings

Supported.

```erlang
{ok, #{embeddings := Vectors, usage := #{input_tokens := In}}} =
    thinwrap_llm:embed(Config, #{model => <<"<embeddings-deployment>">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors :: [[float()]], one per input, in input order
```

`POST <base_url>/embeddings` with `encoding_format: "float"`; `dimensions` maps to `dimensions` when set. Vectors are returned in input order in `embeddings`.

## Error mapping & passthrough

- Any failure is `{error, #{code := Code, message := Msg}}`; `Code` is one of `rate_limited` / `auth_failed` / `provider_unavailable` / `invalid_request` / `context_length_exceeded` / `content_filtered` / `unknown`, and `status` is present when the provider answered.
- By status: `401`/`403`→`auth_failed`, `402`/`429`→`rate_limited`, `408`/`5xx`→`provider_unavailable`, other `4xx`→`invalid_request` (or `context_length_exceeded` on `error.code == "context_length_exceeded"` / context-length wording, `content_filtered` on content-policy / moderation / content-filter wording).
- `cause` holds the raw decoded body under `<<"raw">>`; a `Retry-After` header rides in `<<"retryAfter">>` (raw) and `<<"retryAfterSeconds">>` (parsed, integer delay or HTTP-date only).
- Sub-baseline request fields (`seed`, penalties, `logit_bias`, `n`, prompt caching) go via `passthrough => #{body => ..., headers => ..., query => ...}` (body merged shallowly, overriding the connector's keys); response extras in `raw`.
