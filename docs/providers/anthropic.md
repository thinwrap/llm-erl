# Anthropic (Erlang)

Chat via the native Anthropic **Messages API** adapter (Claude models), module
`thinwrap_llm_anthropic`. Anthropic does not speak OpenAI's Chat Completions
shape, so this is a full translation layer that still emits the identical
normalized result, stream deltas and connector error — switching to/from
`anthropic` is only the config's `provider` + the input's `model`.

## Quick start

```erlang
Config = #{provider => anthropic,
           api_key => list_to_binary(os:getenv("ANTHROPIC_API_KEY"))},
{ok, #{message := #{content := Text}, finish_reason := Finish, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"claude-opus-5-5">>,
        messages => [#{role => user, content => <<"Hi">>}]
    }).
```

## Configuration

| Key | Type | Required | Notes |
|---|---|---|---|
| `provider` | atom | yes | `anthropic`. |
| `api_key` | `binary()` or `fun(() -> binary())` | yes | Sent as the `x-api-key` header (not `Authorization: Bearer`). The fun form keeps the secret out of crash reports that print the config. |
| `base_url` | `binary()` | no | Overrides `https://api.anthropic.com/v1`. Trailing slash trimmed; `/messages` is appended. |
| `anthropic_version` | `binary()` | no | `anthropic-version` header; default `<<"2023-06-01">>`. |
| `default_max_tokens` | `non_neg_integer()` | no | Used (when `> 0`) if the input has no `max_output_tokens`; otherwise `16000` (current models always think, so a smaller ceiling often ends at `length` before any text; it caps output and is not a charge). Anthropic **requires** `max_tokens`. |
| `headers` | `#{binary() => binary()}` | no | Merged onto every request. |
| `transport` | `{module(), Opts}` | no | Default `{thinwrap_llm_httpc, #{}}` (HTTPS-only, never follows redirects). |

When streaming through the default transport, body bytes that reach the client in the same network read as the response headers (typically the first SSE event) are handed over only with the next read: an OTP `httpc` limitation, detailed in the `thinwrap_llm_httpc` module doc.

A missing `api_key`, or a key from another provider's config, raises `badarg`
before any HTTP.

## Auth setup

Create an API key in the Anthropic Console. It is sent as the `x-api-key` header
along with `anthropic-version`. No environment inference — pass `api_key`
explicitly (read it from your own env as above).

## Chat

### Endpoint

`POST <base_url>/messages` (default `https://api.anthropic.com/v1/messages`).

### Notes / quirks

- **System messages** → hoisted into the top-level `system` field (multiple are concatenated with blank lines).
- **`role => tool` messages** → carried inside a `user` turn as `tool_result` blocks; consecutive tool results coalesce into one turn.
- **Assistant `tool_calls`** → `tool_use` blocks; **`tools`** → `{name, description, input_schema}` (the JSON Schema in the tool's `parameters` goes directly into `input_schema`).
- **`tool_choice`** → `auto`→`{type:"auto"}`, `required`→`{type:"any"}`, `none`→`{type:"none"}`, `function`→`{type:"tool", name}`. Claude Opus 5.5, Sonnet 5.5 and Fable 5.1 reject forced tool use (`required` or a named tool) with a 400; that is the vendor's rule, so it surfaces as `invalid_request` and is never emulated.
- **`temperature` / `top_p`** are sent as given. Claude Opus 4.7 and later, every 5.x model and Fable reject them with a 400 (on Sonnet 5.5 and Haiku 5.5, non-default values), which surfaces as `invalid_request`.
- **Images** (`content_parts` with `type => image`) → `{type:"image", source:{type:"base64", media_type, data}}`. Empty text parts are dropped, since Anthropic rejects an empty text block.
- **`stop`** → `stop_sequences`.
- **`reasoning => #{effort => ...}`** → adaptive thinking: `thinking:{type:"adaptive"}` plus `output_config:{effort}`, for every model. An empty effort (`<<>>`) sends `medium`; any other value is sent as given for Anthropic to validate. There is no thinking budget, so `max_tokens` is sent unchanged (default `16000`). While reasoning is set, `temperature` / `top_p` are **omitted**.
- A passthrough `output_config` (for example `#{format => ...}` for structured output) **replaces** the connector's whole `output_config`, effort included (the body merge is shallow); put `effort` in your own `output_config` too.
- **Models before Claude 4.6** do not support adaptive thinking (verified on Haiku 4.5, which returns a 400 `invalid_request`). For those budget-mode models, leave `reasoning` unset and pass the thinking config yourself: `passthrough => #{body => #{thinking => #{type => enabled, budget_tokens => N}}}`, with `max_output_tokens` above `N`.
- **`response_format` is intentionally NOT mapped** — Anthropic has no `response_format`. Use tool-based structured output, supplying the tool via `tools` or raw fields via `passthrough => #{body => ...}`.
- **Reasoning CoT output** — `thinking` blocks are not normalized into `content`; read them from the result's `raw`.
- **Thinking blocks across a tool loop** — the result's `message.provider_metadata` is `#{<<"anthropic">> => #{<<"content">> => Blocks}}`, the response's content blocks verbatim (thinking and `redacted_thinking` included). Copy `provider_metadata` onto every assistant message you send back; the connector then replays those blocks exactly, but only while they still match the message (the same `tool_use` ids in order with the same inputs, and the same text). Otherwise the turn is rebuilt from `content` / `tool_calls` and loses its earlier reasoning. Anthropic invalidates every thinking block after an edited or stripped turn, and accounts that enforce preserved thinking reject replaying them with a 400, so once one assistant turn is rebuilt, every later assistant turn in the request is rebuilt too. Only the `<<"anthropic">>` key is read; other providers' keys are never sent.
- **Prompt caching** — pass `cache_control` markers via `passthrough => #{body => ...}`. `usage.input_tokens` includes `cache_read_input_tokens` and `cache_creation_input_tokens`; the breakdown stays in `raw`.
- **Streaming** (`thinwrap_llm:stream/4`) is real Anthropic SSE — genuinely incremental, folded in the calling process. Text arrives as `content_delta`, tool calls as `tool_call_delta` (concatenate `arguments_delta` per `index`, the 0-based tool-call ordinal); the terminal delta carries `finish_reason`, `usage` and `provider_metadata` (the content blocks rebuilt from the events, the same array a non-stream call returns). A stream that ends before `message_delta` with a `stop_reason` or `message_stop` is `{error, #{code := provider_unavailable}}`.

```erlang
{ok, _} = thinwrap_llm:stream(Config,
    #{model => <<"claude-opus-5-5">>, messages => [#{role => user, content => <<"Hi">>}]},
    fun(Delta, Acc) ->
        io:put_chars(maps:get(content_delta, Delta, <<>>)),
        Acc
    end, ok).
```

- **`stop_reason`** → `finish_reason`: `end_turn`/`stop_sequence`→`stop`, `max_tokens`/`model_context_window_exceeded`→`length`, `tool_use`→`tool_calls`, `refusal`→`content_filter`, else (`pause_turn`, ...) `unknown`. Anthropic reports no total token count; `usage.total_tokens` is summed.

## Embeddings

No OpenAI-float embeddings surface — `thinwrap_llm:supports(anthropic, embed)`
is `false`, and `thinwrap_llm:embed/2` raises `badarg` (misuse, not an
`{error, _}`).

## Error mapping & passthrough

`code`: `401`/`403`→`auth_failed`, `402` (billing; out of credit, like OpenAI's
429 `insufficient_quota`) / `429`→`rate_limited`, `408` and any `5xx` (including
`529` overloaded)→`provider_unavailable`, every other `4xx` (`400`, `404`, `413`
request too large, ...)→`invalid_request` (or `context_length_exceeded` when the
message is Anthropic's context overflow: "prompt is too long" / "exceed context
limit"). An `error` event mid-stream is classified by its `error.type` (`unknown`
for a type it does not know) and carries the HTTP status the stream arrived with.
The raw decoded body is in `cause` under `<<"raw">>`; a `Retry-After` header (when
present) under `<<"retryAfter">>`, with parsed seconds under `<<"retryAfterSeconds">>`.

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
