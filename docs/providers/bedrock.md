# AWS Bedrock (Erlang)

Chat via the native adapter for Amazon Bedrock's unified **Converse API**, module
`thinwrap_llm_bedrock`. Requests are signed with **hand-rolled AWS SigV4**
(`thinwrap_llm_sigv4`, on OTP `crypto`, zero dependencies — no AWS SDK). Converse
is structurally different from OpenAI, so this is a full translation layer that
still emits the identical normalized result, stream deltas and connector error.

## Quick start

```erlang
Config = #{provider => bedrock,
           region => <<"us-east-1">>,
           access_key_id => list_to_binary(os:getenv("AWS_ACCESS_KEY_ID")),
           %% add session_token => ... for temporary credentials
           secret_access_key => list_to_binary(os:getenv("AWS_SECRET_ACCESS_KEY"))},
%% A cross-Region inference profile id, as listed on AWS's Claude Sonnet 5.5
%% model card; the model has no in-Region on-demand id.
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"us.anthropic.claude-sonnet-5-5">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | yes | `bedrock`. |
| `region` | `binary()` | yes | e.g. `<<"us-east-1">>`; folded into the SigV4 credential scope and the default host. Must be an AWS region name (lowercase letters, digits, hyphens), else the call returns `{error, #{code := invalid_request}}` before any HTTP. No environment inference. |
| `access_key_id` | `binary()` or `fun(() -> binary())` | yes | SigV4 credential. |
| `secret_access_key` | `binary()` or `fun(() -> binary())` | yes | SigV4 credential. |
| `session_token` | `binary()` or `fun(() -> binary())` | no | STS temporary-credential token → `X-Amz-Security-Token`. |
| `base_url` | `binary()` | no | Overrides `https://bedrock-runtime.<region>.amazonaws.com` (VPC endpoint / proxy). Must yield a host, else `{error, #{code := invalid_request}}`. A path prefix (a gateway mount point) is kept on the wire and included in the SigV4 canonical path. |
| `default_max_tokens` | `non_neg_integer()` | no | Converse `inferenceConfig.maxTokens` when the input has no `max_output_tokens`; default `4096` (Converse requires `maxTokens`). |
| `headers` | `#{binary() => binary()}` | no | Merged onto the request **and folded into the SigV4 signature**. |
| `transport` | `{module(), Opts}` | no | Default `{thinwrap_llm_httpc, #{}}` (HTTPS-only, never follows redirects). |

When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc.

A missing `region` / `access_key_id` / `secret_access_key`, or a key from another
provider's config, raises `badarg` before any HTTP.

## Auth setup

Uses standard AWS SigV4 (service `bedrock`) on OTP `crypto` — no AWS SDK. Supply
`access_key_id` / `secret_access_key` (and `session_token` for temporary
credentials); the fun form resolves the credential at call time. The body is
serialized once and those exact bytes are signed together with the canonical
path so URL, body, and signature all agree. Headers from `headers` and
`passthrough` are merged case-insensitively before signing (one value per name,
the last one wins), and `Content-Type` is always signed and sent as
`application/json`.

## Chat

### Endpoint

`POST <origin>/model/{modelId}/converse` where `origin` defaults to
`https://bedrock-runtime.<region>.amazonaws.com`. The model id is
percent-encoded once for the wire path (e.g. `...-v2:0` → `...-v2%3A0`) and once
more for the SigV4 canonical URI, because AWS re-encodes the received path before
recomputing the signature. A passthrough query is folded into both the wire URL
and the canonical query string (RFC 3986-encoded, sorted by key).

### Notes / quirks

- **System messages** → top-level `system: [{text}]`.
- **`role => tool` messages** → `toolResult` blocks inside a `user` turn; consecutive tool results coalesce into one turn.
- **Assistant `tool_calls`** → `toolUse` blocks; **`tools`** → `toolConfig.tools[].toolSpec` with the JSON Schema under `inputSchema.json`.
- **`tool_choice`** → `auto`→`{auto:{}}`, `required`→`{any:{}}`, `function`→`{tool:{name}}`; `none` → the whole `toolConfig` is omitted, since Converse has no "none" and withholding the tools is the only way to keep the model from calling one. Bedrock may reject that with a 400 (`invalid_request`) when the history already holds `toolUse` / `toolResult` blocks, as at the end of a tool loop, because it expects `toolConfig` alongside them; use `auto` there.
- **Images** (`content_parts` with `type => image`) → `{image:{format, source:{bytes}}}` (format derived from `media_type`). Empty text parts are dropped, since Converse rejects a blank text block.
- **Sampling** → `inferenceConfig` `{maxTokens, temperature, topP, stopSequences}`.
- **`response_format` is NOT mapped** — use tool-based structured output, or pass raw fields via `passthrough => #{body => ...}`.
- **Reasoning is NOT auto-mapped** — reasoning control is model-dependent on Bedrock; pass `additionalModelRequestFields` via `passthrough => #{body => ...}`.
- **Prompt caching** — `cachePoint` blocks via `passthrough => #{body => ...}`. `top_k` is not mapped either.
- **Streaming** (`thinwrap_llm:stream/4`) is a **non-incremental fallback.** Bedrock's `converse-stream` uses AWS's binary event-stream framing (`application/vnd.amazon.eventstream`), not SSE, and this port does not parse it. `stream/4` issues one `/converse` call and folds the whole completion as deltas: an optional content delta, one `tool_call_delta` per tool call, then a terminal delta carrying `finish_reason` + `usage` + `raw`. A true binary-event-stream parser is a documented follow-up.

```erlang
{ok, _} = thinwrap_llm:stream(Config,
    #{model => <<"us.anthropic.claude-sonnet-5-5">>,
      messages => [#{role => user, content => <<"Hi">>}]},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

- **`stopReason`** → `finish_reason`: `end_turn`/`stop_sequence`→`stop`, `max_tokens`/`model_context_window_exceeded`→`length`, `tool_use`→`tool_calls`, `content_filtered`/`guardrail_intervened`→`content_filter`, else (`malformed_model_output`, `malformed_tool_use`, ...) `unknown`. `usage.input_tokens` is `inputTokens` plus `cacheReadInputTokens` and `cacheWriteInputTokens`; `total_tokens` is input + output. The result's `model` is the requested model id.

## Embeddings

No OpenAI-float embeddings surface — `thinwrap_llm:supports(bedrock, embed)` is
`false`, and `thinwrap_llm:embed/2` raises `badarg` (misuse, not an
`{error, _}`). (Amazon Titan embeddings are not a Converse/OpenAI-float surface.)

## Error mapping & passthrough

`code`: `401`/`403`→`auth_failed`, `429` or a `ThrottlingException` (from the
`x-amzn-ErrorType` header or body `__type`)→`rate_limited`, `>=500` and `408`
(`ModelTimeoutException`) / `424` (`ModelErrorException`)→`provider_unavailable`;
`400`/`404`/`422`→`invalid_request`, except when the message mentions "throttl"
(→`rate_limited`) or is a context overflow ("input is too long", "prompt is too
long", "too many input tokens", "maximum context length", "exceed context limit"
→`context_length_exceeded`; Bedrock relays the model provider's wording);
`402`→`rate_limited`, `413`→`invalid_request`; anything else → `unknown`. The raw
decoded body is in `cause` under `<<"raw">>`; a `Retry-After` header (when present)
under `<<"retryAfter">>` / `<<"retryAfterSeconds">>`.

```erlang
case thinwrap_llm:complete(Config, Input) of
    {ok, Result} -> Result;
    {error, #{code := rate_limited, cause := Cause}} ->
        maps:get(<<"retryAfterSeconds">>, Cause, undefined);
    {error, #{code := Code} = E} -> {Code, maps:get(status, E, undefined)}
end.
```

Provider-specific request fields go through `passthrough => #{body => ...,
headers => ..., query => ...}`; vendor response extras stay in the result's `raw`.
