-module(thinwrap_llm_input).
-moduledoc false.

%% Validation and defaults for configs and operation inputs. Everything here
%% is misuse detection, so every failure raises `{badarg, Description}`
%% before any HTTP — never a connector error. Descriptions never echo a value:
%% a config value may be a credential and an input value may be a prompt.
%%
%% Validated inputs come back normalized: optional keys with a sibling-library
%% default (`content => <<>>`, `tools => []`, …) are filled in, header and
%% passthrough keys are binaries, and `reasoning.effort` is a binary.

-export([config/1, chat_input/1, embeddings_input/1, secret/2]).

-define(NATIVE, [anthropic, bedrock, gemini]).

-doc false.
-spec config(term()) -> thinwrap_llm:config().
config(#{provider := P0} = C) ->
    Compat = thinwrap_llm_spec:compat_providers(),
    P = known_provider(P0, Compat ++ ?NATIVE),
    Keys = config_keys(P, lists:member(P, Compat)),
    Allowed = [provider, transport | [K || {K, _, _} <- Keys]],
    [
        bad(["unknown config key for ", atom_to_list(P), ": ", key_text(K)])
     || K <- maps:keys(C), not lists:member(K, Allowed)
    ],
    C1 = lists:foldl(
        fun({K, Presence, Check}, Acc) ->
            case {maps:find(K, C), Presence} of
                {{ok, V}, _} ->
                    valid(Check, V) orelse
                        bad(["config ", atom_to_list(K), " must be ", describe(Check)]),
                    Acc#{K => norm(Check, V)};
                {error, required} ->
                    bad([atom_to_list(P), " config requires ", atom_to_list(K)]);
                {error, optional} ->
                    Acc
            end
        end,
        #{provider => P},
        Keys
    ),
    C1#{transport => transport(maps:get(transport, C, undefined))};
config(_) ->
    bad(["config must be a map with a provider key"]).

%% `P` itself, now known to be one of `Known`, or a badarg naming them.
-spec known_provider(term(), [thinwrap_llm:provider()]) -> thinwrap_llm:provider().
known_provider(P, Known) ->
    case lists:search(fun(K) -> K =:= P end, Known) of
        {value, Provider} -> Provider;
        false -> bad(["config provider must be one of ", join(Known)])
    end.

config_keys(P, true) ->
    %% These two have no fixed public endpoint, so the caller names it.
    BaseUrl =
        case lists:member(P, ['azure-openai', cloudflare]) of
            true -> required;
            false -> optional
        end,
    [
        {api_key, optional, secret},
        {base_url, BaseUrl, base_url(BaseUrl)},
        {headers, optional, headers}
    ];
config_keys(anthropic, false) ->
    [
        {api_key, required, secret},
        {base_url, optional, binary},
        {anthropic_version, optional, binary}
        | native_tail_keys()
    ];
config_keys(bedrock, false) ->
    [
        {region, required, binary},
        {access_key_id, required, secret},
        {secret_access_key, required, secret},
        {session_token, optional, secret},
        {base_url, optional, binary}
        | native_tail_keys()
    ];
config_keys(gemini, false) ->
    [{api_key, required, secret}, {base_url, optional, binary} | native_tail_keys()].

%% The keys every native provider ends with, in the order they are validated.
native_tail_keys() ->
    [{default_max_tokens, optional, non_neg_integer}, {headers, optional, headers}].

base_url(required) -> nonempty_binary;
base_url(optional) -> binary.

transport(undefined) ->
    {thinwrap_llm_httpc, #{}};
transport({Mod, _Opts} = T) when is_atom(Mod) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} -> ok;
        _ -> bad(["transport module ", atom_to_list(Mod), " cannot be loaded"])
    end,
    erlang:function_exported(Mod, request, 2) orelse
        bad(["transport module ", atom_to_list(Mod), " does not export request/2"]),
    T;
transport(_) ->
    bad(["config transport must be {Module, Opts}"]).

-doc false.
%% Resolve a credential (binary or fun/0) at the point of use; an absent
%% optional credential is `<<>>`.
-spec secret(thinwrap_llm:config(), atom()) -> binary().
secret(Config, Key) ->
    case maps:get(Key, Config, <<>>) of
        F when is_function(F, 0) ->
            case F() of
                B when is_binary(B) -> B;
                _ -> bad(["config ", atom_to_list(Key), " fun must return a binary"])
            end;
        %% config/1 admitted only a binary or a fun/0 under a credential key.
        B when is_binary(B) ->
            B
    end.

%% ---------------------------------------------------------------------------
%% Inputs: {Key, required | {default, V} | optional, Check}

spec(chat) ->
    [
        {model, required, binary},
        {messages, required, {list, {obj, message}}},
        {tools, {default, []}, {list, {obj, tool}}},
        {tool_choice, optional, {obj, tool_choice}},
        {response_format, optional, {obj, response_format}},
        {temperature, optional, number},
        {top_p, optional, number},
        {max_output_tokens, optional, integer},
        {stop, {default, []}, {list, binary}},
        {reasoning, optional, {obj, reasoning}},
        {passthrough, optional, passthrough}
    ];
spec(embeddings) ->
    [
        {model, required, binary},
        {input, required, {list, binary}},
        {dimensions, optional, integer},
        {passthrough, optional, passthrough}
    ];
spec(message) ->
    [
        {role, required, {enum, [system, user, assistant, tool]}},
        {content, {default, <<>>}, binary},
        {content_parts, {default, []}, {list, {obj, content_part}}},
        {tool_calls, {default, []}, {list, {obj, tool_call}}},
        {tool_call_id, {default, <<>>}, binary},
        {name, {default, <<>>}, binary},
        {provider_metadata, optional, map}
    ];
spec(content_part) ->
    [
        {type, required, {enum, [text, image]}},
        {text, {default, <<>>}, binary},
        {base64, {default, <<>>}, binary},
        {media_type, {default, <<>>}, binary}
    ];
spec(tool_call) ->
    [
        {id, required, binary},
        {name, required, binary},
        {arguments, required, binary},
        {provider_metadata, optional, map}
    ];
spec(tool) ->
    [
        {name, required, binary},
        {description, {default, <<>>}, binary},
        {parameters, optional, map}
    ];
spec(tool_choice) ->
    [
        {mode, required, {enum, [auto, none, required, function]}},
        {function_name, {default, <<>>}, binary}
    ];
spec(response_format) ->
    [
        {type, required, {enum, [text, json_object, json_schema]}},
        {json_schema_name, {default, <<>>}, binary},
        {json_schema, optional, map}
    ];
spec(reasoning) ->
    [{effort, required, effort}].

-doc false.
-spec chat_input(term()) -> thinwrap_llm:chat_input().
chat_input(Input) -> obj("complete", chat, Input).

-doc false.
-spec embeddings_input(term()) -> thinwrap_llm:embeddings_input().
embeddings_input(Input) -> obj("embed", embeddings, Input).

obj(Ctx, Name, Input) when is_map(Input) ->
    Spec = spec(Name),
    Known = [K || {K, _, _} <- Spec],
    [
        bad([Ctx, ": unknown key ", key_text(K)])
     || K <- maps:keys(Input), not lists:member(K, Known)
    ],
    lists:foldl(
        fun({K, Presence, Check}, Acc) ->
            Path = [Ctx, ".", atom_to_list(K)],
            case {maps:find(K, Input), Presence} of
                {{ok, V}, _} ->
                    Acc#{K => check(Path, Check, V)};
                {error, required} ->
                    bad([Ctx, ": missing required key ", atom_to_list(K)]);
                {error, {default, D}} ->
                    Acc#{K => D};
                {error, optional} ->
                    Acc
            end
        end,
        #{},
        Spec
    );
obj(Ctx, _Name, _) ->
    bad([Ctx, " must be a map"]).

check(Path, {obj, Name}, V) ->
    obj(Path, Name, V);
check(Path, {list, C}, L) when is_list(L) ->
    [check([Path, "[]"], C, V) || V <- L];
check(Path, Check, V) ->
    valid(Check, V) orelse bad([Path, " must be ", describe(Check)]),
    norm(Check, V).

valid(binary, V) ->
    is_binary(V);
valid(nonempty_binary, V) ->
    is_binary(V) andalso V =/= <<>>;
valid(number, V) ->
    is_number(V);
valid(integer, V) ->
    is_integer(V);
valid(non_neg_integer, V) ->
    is_integer(V) andalso V >= 0;
valid(map, V) ->
    is_map(V);
valid(secret, V) ->
    is_binary(V) orelse is_function(V, 0);
valid(headers, #{} = H) ->
    lists:all(fun valid_header/1, maps:to_list(H));
valid(headers, _) ->
    false;
valid({enum, Vs}, V) ->
    lists:member(V, Vs);
valid({list, _}, _) ->
    false;
valid(effort, V) ->
    is_binary(V) orelse lists:member(V, [low, medium, high]);
valid(passthrough, #{} = P) ->
    lists:all(fun(K) -> lists:member(K, [body, headers, query]) end, maps:keys(P)) andalso
        is_map(maps:get(body, P, #{})) andalso
        valid(headers, maps:get(headers, P, #{})) andalso
        valid_query(maps:get(query, P, #{}));
valid(passthrough, _) ->
    false.

valid_header({K, V}) -> (is_binary(K) orelse is_atom(K)) andalso is_binary(V).

valid_query(#{} = Q) ->
    lists:all(
        fun({K, V}) ->
            (is_binary(K) orelse is_atom(K)) andalso
                (is_binary(V) orelse is_atom(V) orelse is_number(V))
        end,
        maps:to_list(Q)
    );
valid_query(_) ->
    false.

norm(headers, H) -> maps:from_list([{thinwrap_llm_util:bin(K), V} || K := V <- H]);
norm(effort, E) -> thinwrap_llm_util:bin(E);
norm(passthrough, P) -> maps:map(fun norm_pt/2, P);
norm(_, V) -> V.

norm_pt(headers, H) -> norm(headers, H);
norm_pt(_, V) -> V.

describe(binary) -> "a binary";
describe(nonempty_binary) -> "a non-empty binary";
describe(number) -> "a number";
describe(integer) -> "an integer";
describe(non_neg_integer) -> "a non-negative integer";
describe(map) -> "a map";
describe(secret) -> "a binary or a fun/0";
describe(headers) -> "a map of binary or atom names to binary values";
describe({enum, Vs}) -> ["one of ", join(Vs)];
describe({list, C}) -> ["a list of ", describe(C)];
describe({obj, Name}) -> ["a ", atom_to_list(Name), " map"];
describe(effort) -> "low, medium, high or a binary";
describe(passthrough) -> "#{body => map(), headers => map(), query => map()}".

join(Atoms) -> lists:join(", ", [atom_to_list(A) || A <- Atoms]).

key_text(K) when is_atom(K) -> atom_to_list(K);
key_text(_) -> "(non-atom key)".

-spec bad(iodata()) -> no_return().
bad(IoList) ->
    erlang:error({badarg, unicode:characters_to_binary(IoList)}).
