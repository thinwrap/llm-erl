# Google Gemini (Erlang)

Chat via the native adapter for the Google Gemini (**AI Studio**)
`generateContent` API, module `thinwrap_llm_gemini`. Gemini uses
`contents[]`/`parts[]` (roles `user`/`model`), a top-level `systemInstruction`,
and `functionDeclarations`, so this is a full translation layer that still emits
the identical normalized result, stream deltas and connector error. Unlike the
Bedrock fallback, Gemini streaming is **real SSE** — genuinely incremental.

> This is the AI Studio Gemini API (`gemini` provider). Vertex AI Gemini is a
> separate product with different auth (GCP OAuth/ADC) and endpoints — not this
> connector.

## Quick start

```erlang
Config = #{provider => gemini,
           api_key => list_to_binary(os:getenv("GEMINI_API_KEY"))},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"gemini-3.8-flash">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | yes | `gemini`. |
| `api_key` | `binary()` or `fun(() -> binary())` | yes | Sent as the `x-goog-api-key` header. The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | no | Overrides `https://generativelanguage.googleapis.com/v1beta`. Trailing slash trimmed. |
| `default_max_tokens` | `non_neg_integer()` | no | `generationConfig.maxOutputTokens` when the input has no `max_output_tokens`. No hard default — if both are unset, `maxOutputTokens` is simply omitted. |
| `headers` | `#{binary() => binary()}` | no | Merged onto every request. |
| `transport` | `{module(), Opts}` | no | Default `{thinwrap_llm_httpc, #{}}` (HTTPS-only, never follows redirects). |

When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc.

A missing `api_key`, or a key from another provider's config, raises `badarg`
before any HTTP.

## Auth setup

Create an API key in Google AI Studio. It is sent as the `x-goog-api-key` header
(the key is not placed in the query string). No environment inference — pass
`api_key` explicitly.

## Chat

### Endpoint

`POST <base_url>/models/{model}:generateContent` (non-streaming) and
`POST <base_url>/models/{model}:streamGenerateContent?alt=sse` (streaming). The
model is path-escaped. Default base URL
`https://generativelanguage.googleapis.com/v1beta`. A passthrough query is
appended with `&` after `?alt=sse`.

### Notes / quirks

- **System messages** → top-level `systemInstruction` (multiple concatenated with blank lines).
- **Roles** → `assistant`→`model`, everything else → `user`.
- **`role => tool` messages** → `functionResponse` parts inside a `user` turn, keyed by function **NAME**. The name is resolved from the message's `name`, else the matching tool call in the nearest preceding assistant turn (`tool_call_id`→name; synthetic ids repeat from turn to turn), else the raw `tool_call_id`, else `"tool"`. Consecutive tool results coalesce into one turn.
- **Tool-call ids** — when Gemini returns a `functionCall.id`, it becomes the tool call's `id` and is sent back as `functionCall.id` / `functionResponse.id`. Otherwise the id is synthetic and index-based (`<<"call_0">>`, `<<"call_1">>`, ... in both `complete` and `stream`, so parallel calls to one function stay distinct), and synthetic ids are never sent to Gemini.
- **Assistant `tool_calls`** → `functionCall` parts; **`tools`** → `[{functionDeclarations: [...]}]`, with the tool's `parameters` sent as `parametersJsonSchema` (full JSON Schema, e.g. `additionalProperties`; the older OpenAPI-subset `parameters` field rejects that with a 400).
- **Thought signatures (Gemini 3)** — Gemini rejects a replayed `functionCall` without its `thoughtSignature` (400 "Function call is missing a thought_signature"). The signature arrives on the tool call's `provider_metadata` as `#{<<"gemini">> => #{<<"thoughtSignature">> => Sig}}` (on `tool_call_delta`'s `provider_metadata` when streaming; copy it onto the assembled tool call). Send the tool calls back unchanged and the connector puts each signature back on its `functionCall` part. When no call in a model turn has one (a hand-built history, or one from another provider), the first `functionCall` of that turn gets Google's documented `"skip_thought_signature_validator"` value so the loop keeps working. Only the `<<"gemini">>` key is read; other providers' keys are ignored.
- **`tool_choice`** → `toolConfig.functionCallingConfig.mode`: `auto`→`AUTO`, `required`→`ANY`, `none`→`NONE`, `function`→`ANY` + `allowedFunctionNames`.
- **Images** (`content_parts` with `type => image`) → `{inlineData:{mimeType, data}}`.
- **Sampling** → `generationConfig` `{maxOutputTokens, temperature, topP, stopSequences}`.
- **`response_format` IS mapped** (unlike Anthropic/Bedrock): `json_object`→`responseMimeType: application/json`; `json_schema`→ that plus `responseJsonSchema` (full JSON Schema; when the format's `json_schema` is set).
- **`reasoning.effort`** → `generationConfig.thinkingConfig.thinkingBudget`: `low`=1024, `medium`=8192, `high`=24576 (any other value → 8192, since a budget of 0 turns thinking off).
- **Prompt caching** (`cachedContent`), safety settings, and other model params → `passthrough => #{body => ...}`; reasoning CoT (`thought` parts) is not normalized into `content` and stays in `raw`.
- **Streaming** (`thinwrap_llm:stream/4`) is real SSE, folded in the calling process. Text arrives as `content_delta`, each `functionCall` as its own `tool_call_delta`. Gemini repeats running usage totals on every chunk, so `finish_reason` and the final `usage` come once, on the terminal delta. A stream that ends without a `finishReason` (or a prompt-level block) is `{error, #{code := provider_unavailable}}`.

```erlang
{ok, _} = thinwrap_llm:stream(Config,
    #{model => <<"gemini-3.8-flash">>, messages => [#{role => user, content => <<"Hi">>}]},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

- **`finishReason`** → `finish_reason`: `STOP`→`stop`, `MAX_TOKENS`→`length`, `SAFETY`/`RECITATION`/`BLOCKLIST`/`PROHIBITED_CONTENT`/`SPII`/`IMAGE_SAFETY`/`IMAGE_PROHIBITED_CONTENT`/`IMAGE_RECITATION`→`content_filter`, `CONTINUATION`→`length`, else (`LANGUAGE`, `MALFORMED_FUNCTION_CALL`, `UNEXPECTED_TOOL_CALL`, `OTHER`, ...) `unknown`; a `functionCall` part forces `tool_calls`. A prompt-level block (no candidate, only `promptFeedback.blockReason`) is `content_filter`. Usage maps from `usageMetadata`: `input_tokens`=`promptTokenCount`+`toolUsePromptTokenCount`, `output_tokens`=`candidatesTokenCount`+`thoughtsTokenCount` (thinking is billed as output), `total_tokens`=input + output.

## Embeddings

No OpenAI-float embeddings surface — `thinwrap_llm:supports(gemini, embed)` is
`false`, and `thinwrap_llm:embed/2` raises `badarg` (misuse, not an
`{error, _}`).

## Error mapping & passthrough

`code`: `401`/`403`→`auth_failed`, `402` (prepay credit depleted,
`RESOURCE_EXHAUSTED`) / `429`→`rate_limited`, `408` / `>=500`→`provider_unavailable`,
every other `4xx`→`invalid_request` (or `auth_failed` for a 400 whose `ErrorInfo`
reason is `API_KEY_INVALID`, or `context_length_exceeded` when the message is
Gemini's input-token overflow: "exceeds the maximum number of tokens"). An error
object mid-stream is classified by its `code`, else its `status`, else `unknown`,
and carries the HTTP status the stream arrived with. Gemini emits **no rate-limit
headers** — retry timing is parsed from the 429 body's `error.details`
`RetryInfo.retryDelay` (e.g. `"12s"`) into `cause`'s `<<"retryAfterSeconds">>`
when a `Retry-After` header did not already populate it. The raw decoded body is
in `cause` under `<<"raw">>`.

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
