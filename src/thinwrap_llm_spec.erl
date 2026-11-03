-module(thinwrap_llm_spec).
-moduledoc false.

%% Per-provider parameterization of the one shared OpenAI-compatible
%% connector. Adding a first-class OpenAI-compatible provider is a row here,
%% not a new module.

-export([get/1, compat_providers/0, embeddings_providers/0]).

-export_type([spec/0]).

-type spec() :: #{
    id := thinwrap_llm:provider(),
    default_base_url := binary(),
    auth := bearer | azure,
    supports_tools := boolean(),
    max_tokens_field := binary(),
    %% `x_groq` relocates streaming usage.
    stream_usage_path := none | x_groq,
    strict_params := boolean(),
    %% Embeddings output-size field.
    dimensions_field := binary()
}.

-doc false.
-spec compat_providers() -> [thinwrap_llm:provider()].
compat_providers() ->
    [
        openai,
        'azure-openai',
        openrouter,
        groq,
        together,
        fireworks,
        deepseek,
        xai,
        mistral,
        perplexity,
        deepinfra,
        cloudflare,
        vllm,
        ollama,
        lmstudio
    ].

-doc false.
%% First-class providers exposing an OpenAI-float /embeddings surface: every
%% compat provider except the four that have none, in the same order.
-spec embeddings_providers() -> [thinwrap_llm:provider()].
embeddings_providers() ->
    [P || P <- compat_providers(), not lists:member(P, [groq, deepseek, xai, perplexity])].

-doc false.
-spec get(thinwrap_llm:provider()) -> spec().
%% OpenAI deprecates `max_tokens` in favor of `max_completion_tokens` (the only
%% one o-series/reasoning models accept); Azure's reasoning models on the v1
%% API likewise reject `max_tokens`.
get(openai) ->
    row(openai, <<"https://api.openai.com/v1">>, #{
        max_tokens_field => <<"max_completion_tokens">>
    });
get('azure-openai') ->
    row('azure-openai', <<>>, #{auth => azure, max_tokens_field => <<"max_completion_tokens">>});
get(openrouter) ->
    row(openrouter, <<"https://openrouter.ai/api/v1">>, #{});
get(groq) ->
    row(groq, <<"https://api.groq.com/openai/v1">>, #{stream_usage_path => x_groq});
get(together) ->
    row(together, <<"https://api.together.ai/v1">>, #{});
get(fireworks) ->
    row(fireworks, <<"https://api.fireworks.ai/inference/v1">>, #{});
get(deepseek) ->
    row(deepseek, <<"https://api.deepseek.com/v1">>, #{});
get(xai) ->
    row(xai, <<"https://api.x.ai/v1">>, #{});
%% Mistral's embeddings API has no `dimensions`; its output size is
%% `output_dimension`. `strict_params`: `stream_options` is never sent.
get(mistral) ->
    row(mistral, <<"https://api.mistral.ai/v1">>, #{
        strict_params => true, dimensions_field => <<"output_dimension">>
    });
get(perplexity) ->
    row(perplexity, <<"https://api.perplexity.ai">>, #{supports_tools => false});
get(deepinfra) ->
    row(deepinfra, <<"https://api.deepinfra.com/v1/openai">>, #{});
get(cloudflare) ->
    row(cloudflare, <<>>, #{});
get(vllm) ->
    row(vllm, <<"http://localhost:8000/v1">>, #{});
get(ollama) ->
    row(ollama, <<"http://localhost:11434/v1">>, #{});
get(lmstudio) ->
    row(lmstudio, <<"http://localhost:1234/v1">>, #{}).

row(Id, Base, Overrides) ->
    maps:merge(
        #{
            id => Id,
            default_base_url => Base,
            auth => bearer,
            supports_tools => true,
            max_tokens_field => <<"max_tokens">>,
            stream_usage_path => none,
            strict_params => false,
            dimensions_field => <<"dimensions">>
        },
        Overrides
    ).
