# thinwrap_llm (Erlang)

Unified, **SDK-free**, **zero-dependency** Erlang wrapper for **chat
completions** (with streaming) and **embeddings** across 18 LLM providers.
Switch vendor by changing the config's `provider` + the input's `model`; the
input and output shapes stay identical. Synchronous, stateless, OTP-only — bring
your own HTTP client if you want `hackney`, `gun` or `Finch`. Works the same from
Elixir and Gleam.

```erlang
%% rebar.config
{deps, [thinwrap_llm]}.
```

```elixir
# mix.exs
{:thinwrap_llm, "~> 1.1"}
```

Requires Erlang/OTP ≥ 27. No runtime dependencies — Bedrock's SigV4 is
hand-rolled on `crypto`, SSE is parsed by the library, JSON is OTP `json`.

## Two-minute chat

```erlang
Config = #{provider => openai, api_key => <<"...">>},
case thinwrap_llm:complete(Config, #{
    model => <<"gpt-4o-mini">>,
    messages => [#{role => user, content => <<"Say hi in one word.">>}]
}) of
    {ok, #{message := #{content := Text}, usage := #{total_tokens := N}}} ->
        io:format("~ts (~B tokens)~n", [Text, N]);
    {error, #{code := Code, message := Msg}} ->
        io:format("~s: ~ts~n", [Code, Msg])
end.
```

```elixir
{:ok, %{message: %{content: text}}} =
  :thinwrap_llm.complete(%{provider: :anthropic, api_key: key}, %{
    model: "claude-opus-5-5",
    messages: [%{role: :user, content: "Say hi in one word."}]
  })
```

## Streaming

`stream/4` folds your fun over the normalized deltas **in the calling
process** as they arrive and leaves nothing in your mailbox. The library starts
no long-lived processes; the default `httpc` transport runs each streaming call
through one helper process that ends with the call (a transport of your own is
unaffected):

```erlang
{ok, _} = thinwrap_llm:stream(Config, Input,
    fun(#{content_delta := Chunk}, Acc) -> io:put_chars(Chunk), Acc;
       (_Delta, Acc) -> Acc
    end, ok).
```

A stream that is cut off before the provider's terminal event, or a 2xx answer
that is not `text/event-stream`, is `{error, #{code := provider_unavailable}}`
rather than ending as if it were complete; deltas already passed to your fun
stay delivered. If your fun raises, the request is cancelled and the exception
propagates unchanged.

## Usage

`usage` counts every billed token: `input_tokens` includes cache reads and
writes (Anthropic, Bedrock), `output_tokens` includes thinking tokens (Gemini),
and `total_tokens` is the vendor's total, or input + output when it reports
none. When streaming, `usage` arrives once, on the terminal delta.

## Tool loops and `provider_metadata`

Some providers need opaque state from a response sent back on the next turn:
Gemini 3 rejects a replayed function call without its thought signature, and
Anthropic keeps its thinking blocks only if they are replayed verbatim.
`provider_metadata` on each tool call and on the assistant message carries that
state, keyed by provider (`#{<<"gemini">> => ...}`, `#{<<"anthropic">> => ...}`).
Treat it as opaque and copy it into the next request together with `content`
and `tool_calls`:

```erlang
{ok, #{message := Msg}} = thinwrap_llm:complete(Config, Input#{tools => Tools}),
Next = Messages ++ [maps:with([role, content, tool_calls, provider_metadata], Msg)].
```

When streaming, the state rides on `tool_call_delta`'s `provider_metadata`
(copy it onto the tool call you assemble) and on the terminal delta. A
connector reads only its own key, so a history survives a provider switch.

## Providers

The config's **`provider` selects the connector**; inputs and results are
identical across all of them. `thinwrap_llm:supports(Provider, complete |
stream | embed)` answers at runtime.

**Chat (18):** 15 OpenAI-compatible — `openai`, `'azure-openai'`, `openrouter`,
`groq`, `together`, `fireworks`, `deepseek`, `xai`, `mistral`, `perplexity`,
`deepinfra`, `cloudflare`, `vllm`, `ollama`, `lmstudio` (config: `api_key`,
`base_url` — required for `'azure-openai'` and `cloudflare` — and `headers`);
plus 3 natives: `anthropic` (`api_key`), `bedrock` (`region`, `access_key_id`,
`secret_access_key`, optional `session_token`), `gemini` (`api_key`).

**Embeddings (11):** `openai`, `'azure-openai'`, `openrouter`, `together`,
`fireworks`, `mistral`, `deepinfra`, `cloudflare`, `vllm`, `ollama`, `lmstudio`.

```erlang
{ok, #{embeddings := Vectors}} =
    thinwrap_llm:embed(#{provider => openai, api_key => Key},
                       #{model => <<"text-embedding-3-small">>, input => [<<"hello">>, <<"world">>]}).
%% Vectors is a list of float lists, in input order.
```

Any credential may be a binary or a zero-arity fun returning one, which keeps
the secret out of crash reports that print the config. Per-provider details
live in [`docs/providers/`](docs/providers/).

## The ≥90% baseline & passthrough

Only fields ≥90% of providers support normalizably are first-class on the input
and result; everything else rides through `passthrough` (request) / `raw`
(response) and is **never emulated**:

```erlang
{ok, #{raw := Raw}} = thinwrap_llm:complete(Config, Input#{
    passthrough => #{body => #{logprobs => true}}
}).
```

## Errors

Every **operation** failure is `{error, Error}`, a plain map
`#{code, message, status?, provider_message?, cause?}`. `code` is one of
`rate_limited`, `auth_failed`, `provider_unavailable`, `invalid_request`,
`context_length_exceeded`, `content_filtered`, `unknown`. The raw `Retry-After`
rides in `cause` as `<<"retryAfter">>` (+ parsed `<<"retryAfterSeconds">>`) —
there is no top-level retry key.

By HTTP status: `401`/`403` → `auth_failed`; `402` (out of credit) / `429` →
`rate_limited`; `408` / `5xx` → `provider_unavailable`; every other `4xx` →
`invalid_request`, unless the vendor's context-overflow or content-filter signal
matches. An error inside a 2xx body or mid-stream keeps the HTTP status it
arrived with and is classified by its own code or type.

**Misuse** — an unknown or missing config/input key, a wrong type, an atom
outside its set, a config for another provider, embeddings on a chat-only
provider — raises `error({badarg, Description})` before any request. The
description never includes config values.

## Bring your own transport

```erlang
Config = #{provider => openai, api_key => Key, transport => {my_transport, Opts}}.
```

`my_transport` implements the `thinwrap_llm_transport` behaviour:
`request/2` for one request/response, and optionally `stream/4` for true
incremental streaming (without it a buffered fallback is used). The default,
`thinwrap_llm_httpc`, does both on OTP `httpc`, never follows redirects, names
itself in `User-Agent`, and is HTTPS-only: the `http://` defaults of local
servers (vLLM, Ollama, LM Studio) get an `invalid_request` telling you to bring
your own transport. Its timeout is 600 s (whole request; idle time between
chunks when streaming), so a long non-streaming reasoning call is not cut off;
pass `{thinwrap_llm_httpc, #{timeout => Ms}}` for a different budget. The
wrapper is stateless — no caching, retries, or conversation memory.

## Relationship to the siblings

One of five thinwrap llm libraries sharing a normalized surface and
byte-identical provider ids / error codes: TypeScript (`@thinwrap/llm`), PHP
(`thinwrap/llm`), Go (`thinwrap/llm-go`), Python (`thinwrap-llm`), Erlang (this
package).

## Security

Report vulnerabilities **privately** — please do not open a public issue.
Preferred: a [private security advisory](https://github.com/thinwrap/llm-erl/security/advisories/new)
on this repository. Alternatively, email **security@thinwrap.dev**. Include the
affected versions and a minimal reproduction if you have one.

A vulnerability in a *provider's* own API or service belongs to that vendor
rather than to this wrapper — please report those upstream.

## License

MIT © Dmitry Polyanovsky & contributors
