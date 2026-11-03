-module(thinwrap_llm).
-moduledoc """
Chat completions (with streaming) and embeddings across 18 LLM providers —
one normalized surface, zero dependencies.

Switching vendor is a change of the config's `provider` + the input's `model`
only; the input and output shapes are identical across every provider.

```erlang
Config = #{provider => openai, api_key => <<"sk-...">>},
{ok, #{message := #{content := Text}, usage := #{total_tokens := N}}} =
    thinwrap_llm:complete(Config, #{
        model => <<"gpt-4o-mini">>,
        messages => [#{role => user, content => <<"Say hi in one word.">>}]
    }).
```

## Streaming

`stream/4` folds a fun over the normalized deltas **in the calling process**
as they arrive. The library starts no long-lived processes; the default
`thinwrap_llm_httpc` transport runs each streaming call through one helper
process that ends with the call (a transport of your own is unaffected):

```erlang
{ok, Text} = thinwrap_llm:stream(Config, Input,
    fun(Delta, Acc) -> [Acc, maps:get(content_delta, Delta, <<>>)] end, []).
```

A stream that is cut off before the provider's terminal event, or a 2xx
answer that is not `text/event-stream`, is `{error, #{code :=
provider_unavailable}}` rather than ending as if it were complete. Deltas
already delivered to the fun stay delivered. `usage` arrives once, on the
terminal delta.

## Two kinds of failure

- **Operation failures** — a vendor error, a transport failure, request data
  the vendor would reject — come back as `{error, connector_error()}`, a
  plain map whose `code` you branch on.
- **Misuse** — a missing or unknown config/input key, a value of the wrong
  type, an enum atom outside its set, a provider that lacks the operation —
  raises `error({badarg, Description})` before any HTTP. Folding those into
  `{error, _}` would make a bug in the calling code look like bad end-user
  input or, worse, a retryable outage.

## Usage

`usage` counts every billed token: `input_tokens` includes cache reads and
writes (Anthropic, Bedrock) and tool-use prompt tokens (Gemini),
`output_tokens` includes thinking tokens (Gemini), and `total_tokens` is the
vendor's total, or input + output when it reports none.

## Tool loops and `provider_metadata`

Some providers need opaque state from a response sent back on the next turn:
Gemini 3 rejects a replayed function call without its thought signature, and
Anthropic keeps its thinking blocks only if they are replayed verbatim.
`provider_metadata` on a tool call and on the assistant message carries that
state, keyed by provider (`#{<<"gemini">> => ...}`,
`#{<<"anthropic">> => ...}`). Treat it as opaque and copy it into the next
request together with `content` and `tool_calls`. When streaming, it rides on
`tool_call_delta.provider_metadata` and on the terminal delta. A connector
reads only its own key, so a history survives a provider switch.

## Passthrough

Every input accepts `passthrough => #{body => map(), headers => map(),
query => map()}`, forwarded verbatim and overriding what the connector set
(auth included): `body` is merged shallowly (a key replaces the connector's
whole value), `headers` are merged, `query` is appended to the URL. Vendor
extras on the response are in `raw`.
""".

-export([complete/2, stream/4, embed/2, supports/2]).

-export_type([
    config/0,
    provider/0,
    operation/0,
    secret/0,
    connector_error/0,
    passthrough/0,
    role/0,
    content_part/0,
    tool_call/0,
    message/0,
    tool_definition/0,
    tool_choice/0,
    response_format/0,
    reasoning/0,
    chat_input/0,
    finish_reason/0,
    usage/0,
    assistant_message/0,
    chat_result/0,
    tool_call_delta/0,
    stream_delta/0,
    embeddings_input/0,
    embeddings_usage/0,
    embeddings_result/0,
    result/1
]).

-doc """
Provider ids, byte-identical to the sibling libraries' values. The first 15
share one OpenAI-compatible connector; `anthropic`, `bedrock` and `gemini` are
native adapters.
""".
-type provider() ::
    openai
    | 'azure-openai'
    | openrouter
    | groq
    | together
    | fireworks
    | deepseek
    | xai
    | mistral
    | perplexity
    | deepinfra
    | cloudflare
    | vllm
    | ollama
    | lmstudio
    | anthropic
    | bedrock
    | gemini.

-type operation() :: complete | stream | embed.

%% A credential, or a zero-arity fun returning it. The fun form keeps the
%% secret out of crash reports and logs that print the config map (EEF
%% secure-coding guidance).
-type secret() :: binary() | fun(() -> binary()).

-doc """
Provider configuration. `provider` selects the connector; the other keys
depend on it:

- the 15 OpenAI-compatible providers — `api_key` (optional for the
  self-hosted `vllm`, `ollama`, `lmstudio`; sent as `Authorization: Bearer`,
  or `api-key` for `'azure-openai'`), `base_url` (required for
  `'azure-openai'` and `cloudflare`, which have no fixed public endpoint),
  `headers`;
- `anthropic` — `api_key` (required, sent as `x-api-key`), `base_url`,
  `anthropic_version` (default `<<"2023-06-01">>`), `default_max_tokens`,
  `headers`;
- `bedrock` — `region`, `access_key_id`, `secret_access_key` (required),
  `session_token`, `base_url`, `default_max_tokens`, `headers`;
- `gemini` — `api_key` (required, sent as `x-goog-api-key`), `base_url`,
  `default_max_tokens`, `headers`.

Optional on every provider: `transport` (see `thinwrap_llm_transport`;
default `{thinwrap_llm_httpc, #{}}`).
""".
-type config() :: #{
    provider := provider(),
    api_key => secret(),
    base_url => binary(),
    headers => #{binary() | atom() => binary()},
    anthropic_version => binary(),
    default_max_tokens => non_neg_integer(),
    region => binary(),
    access_key_id => secret(),
    secret_access_key => secret(),
    session_token => secret(),
    transport => thinwrap_llm_transport:transport()
}.

-doc """
The operation-failure value. `code` is the normalized classification shared
with the sibling libraries; `status` is the HTTP status when the provider
answered (an error inside a 2xx body or a stream frame keeps the status it
arrived with); `cause` is a binary-keyed map with the decoded vendor body
under `<<"raw">>`, plus `<<"retryAfter">>` (the raw header) and
`<<"retryAfterSeconds">>` when the provider sent a usable `Retry-After`.
""".
-type connector_error() :: #{
    code := thinwrap_llm_error:code(),
    message := binary(),
    status => integer(),
    provider_message => binary(),
    cause => term()
}.

-type passthrough() :: #{
    body => map(),
    headers => #{binary() | atom() => binary()},
    query => #{binary() | atom() => binary() | atom() | number()}
}.

-type role() :: system | user | assistant | tool.

-doc "One part of a multimodal message.".
-type content_part() ::
    #{type := text, text => binary()}
    | #{type := image, base64 => binary(), media_type => binary()}.

-doc """
A tool call. `arguments` is the JSON-encoded arguments text.
`provider_metadata` is opaque per-provider state keyed by provider id (e.g.
Gemini's thought signature); copy it back unchanged when replaying the call.
""".
-type tool_call() :: #{
    id := binary(),
    name := binary(),
    arguments := binary(),
    provider_metadata => map()
}.

-doc """
One turn. Use `content` for plain text, or `content_parts` for multimodal
(`content_parts` wins when non-empty). `provider_metadata` on an assistant
turn is the opaque state copied from the result's message.
""".
-type message() :: #{
    role := role(),
    content => binary(),
    content_parts => [content_part()],
    tool_calls => [tool_call()],
    tool_call_id => binary(),
    name => binary(),
    provider_metadata => map()
}.

-type tool_definition() :: #{
    name := binary(),
    description => binary(),
    %% JSON Schema; vendor JSON, so binary or atom keys.
    parameters => map()
}.

-doc "`function` needs `function_name`.".
-type tool_choice() :: #{
    mode := auto | none | required | function,
    function_name => binary()
}.

-doc "`json_schema` takes `json_schema_name` + `json_schema`.".
-type response_format() :: #{
    type := text | json_object | json_schema,
    json_schema_name => binary(),
    json_schema => map()
}.

-doc """
Reasoning effort. `low | medium | high`, or a binary sent as given (an empty
binary means "unspecified"). The one deliberate carve-out from the ≥90%
baseline rule; providers that cannot map it ignore it.
""".
-type reasoning() :: #{effort := low | medium | high | binary()}.

-type chat_input() :: #{
    model := binary(),
    messages := [message()],
    tools => [tool_definition()],
    tool_choice => tool_choice(),
    response_format => response_format(),
    temperature => number(),
    top_p => number(),
    max_output_tokens => integer(),
    stop => [binary()],
    reasoning => reasoning(),
    passthrough => passthrough()
}.

-type finish_reason() :: stop | length | tool_calls | content_filter | unknown.

-type usage() :: #{
    input_tokens := integer(),
    output_tokens := integer(),
    total_tokens := integer()
}.

-type assistant_message() :: #{
    role := assistant,
    content := binary(),
    tool_calls := [tool_call()],
    provider_metadata => map()
}.

-doc "`raw` is the decoded vendor body (JSON `null` is the atom `null`).".
-type chat_result() :: #{
    message := assistant_message(),
    finish_reason := finish_reason(),
    usage := usage(),
    model := binary(),
    raw := term()
}.

-doc """
A streamed tool-call fragment. Concatenate `arguments_delta` per `index`
(the 0-based tool-call ordinal); `id` and `function_name` arrive on the first
fragment. Copy `provider_metadata` onto the assembled tool call.
""".
-type tool_call_delta() :: #{
    index := integer(),
    id => binary(),
    function_name => binary(),
    arguments_delta => binary(),
    provider_metadata => map()
}.

-doc """
One normalized stream delta. Every key is optional and omitted when empty.
`usage`, `finish_reason` and the message-level `provider_metadata` arrive on
the terminal delta; `raw` is the decoded vendor event the delta came from.
""".
-type stream_delta() :: #{
    content_delta => binary(),
    tool_call_delta => tool_call_delta(),
    finish_reason => finish_reason(),
    usage => usage(),
    raw => term(),
    provider_metadata => map()
}.

-type embeddings_input() :: #{
    model := binary(),
    input := [binary()],
    dimensions => integer(),
    passthrough => passthrough()
}.

-type embeddings_usage() :: #{input_tokens := integer(), total_tokens := integer()}.

-doc "`embeddings` holds one vector per input, in input order.".
-type embeddings_result() :: #{
    embeddings := [[float()]],
    usage := embeddings_usage(),
    model := binary(),
    raw := term()
}.

-type result(T) :: {ok, T} | {error, connector_error()}.

-doc "Whether `Provider` implements `Operation`.".
-spec supports(provider(), operation()) -> boolean().
supports(Provider, embed) ->
    lists:member(Provider, thinwrap_llm_spec:embeddings_providers());
supports(Provider, Op) when Op =:= complete; Op =:= stream ->
    lists:member(Provider, thinwrap_llm_spec:compat_providers() ++ [anthropic, bedrock, gemini]);
supports(_, _) ->
    false.

-doc "Run one chat completion.".
-spec complete(config(), chat_input()) -> result(chat_result()).
complete(Config0, Input0) ->
    {Config, Mod} = prepare(complete, Config0),
    Input = thinwrap_llm_input:chat_input(Input0),
    run(fun() -> Mod:complete(Config, Input) end).

-doc """
Stream one chat completion, folding `Fun(Delta, Acc)` over the normalized
deltas in the calling process as they arrive. Returns the final accumulator,
or the error that ended the stream.
""".
-spec stream(config(), chat_input(), fun((stream_delta(), Acc) -> Acc), Acc) ->
    {ok, Acc} | {error, connector_error()}.
stream(Config0, Input0, Fun, Acc0) ->
    {Config, Mod} = prepare(stream, Config0),
    Input = thinwrap_llm_input:chat_input(Input0),
    is_function(Fun, 2) orelse erlang:error({badarg, <<"stream fun must be a fun/2">>}),
    run(fun() -> Mod:stream(Config, Input, Fun, Acc0) end).

-doc """
Create embeddings for `input` (the OpenAI-float subset: `openai`,
`'azure-openai'`, `openrouter`, `together`, `fireworks`, `mistral`,
`deepinfra`, `cloudflare`, `vllm`, `ollama`, `lmstudio`).
""".
-spec embed(config(), embeddings_input()) -> result(embeddings_result()).
embed(Config0, Input0) ->
    {Config, _} = prepare(embed, Config0),
    Input = thinwrap_llm_input:embeddings_input(Input0),
    run(fun() -> thinwrap_llm_openai_compat_embeddings:embed(Config, Input) end).

prepare(Op, Config0) ->
    Config = thinwrap_llm_input:config(Config0),
    #{provider := Provider} = Config,
    supports(Provider, Op) orelse
        erlang:error(
            {badarg,
                iolist_to_binary([
                    atom_to_binary(Provider), " does not support ", atom_to_binary(Op)
                ])}
        ),
    {Config, provider_module(Provider)}.

run(F) ->
    try F() of
        Result -> {ok, Result}
    catch
        throw:{thinwrap_llm_error, Error} -> {error, Error}
    end.

provider_module(anthropic) -> thinwrap_llm_anthropic;
provider_module(bedrock) -> thinwrap_llm_bedrock;
provider_module(gemini) -> thinwrap_llm_gemini;
provider_module(_) -> thinwrap_llm_openai_compat.
