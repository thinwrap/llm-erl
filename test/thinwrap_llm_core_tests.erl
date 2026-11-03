-module(thinwrap_llm_core_tests).

%% Ported from llm-py tests/test_core.py and tests/test_dispatch_all.py:
%% compat chat (complete + stream), embeddings, dispatch, error mapping,
%% Retry-After, stream termination, plus the Erlang-specific misuse contract.

-include_lib("eunit/include/eunit.hrl").

%% This module doubles as a transport WITHOUT stream/4 (the buffered
%% fallback) and as transports that crash or break the contract.
-behaviour(thinwrap_llm_transport).
-export([request/2]).

-define(F, thinwrap_llm_fake_transport).
-define(SSE, #{<<"content-type">> => <<"text/event-stream">>}).

%% ---------------------------------------------------------------------------
%% helpers

cfg(Provider, Responses) -> cfg(Provider, #{api_key => <<"k">>}, Responses).

cfg(Provider, Extra, Responses) -> ?F:config(Extra#{provider => Provider}, Responses).

one_user() -> one_user(<<"m">>).
one_user(Model) -> #{model => Model, messages => [#{role => user, content => <<"hi">>}]}.

%% {ok, Deltas} or {error, E}.
collect(C, Input) ->
    case thinwrap_llm:stream(C, Input, fun(D, Acc) -> [D | Acc] end, []) of
        {ok, Ds} -> {ok, lists:reverse(Ds)};
        Err -> Err
    end.

%% The deltas delivered before the stream ended, plus how it ended.
collect_until_error(C, Input) ->
    Self = self(),
    Ref = make_ref(),
    Result = thinwrap_llm:stream(
        C,
        Input,
        fun(D, A) ->
            Self ! {Ref, D},
            A
        end,
        ok
    ),
    {drain(Ref, []), Result}.

drain(Ref, Acc) ->
    receive
        {Ref, D} -> drain(Ref, [D | Acc])
    after 0 -> lists:reverse(Acc)
    end.

text(Deltas) -> iolist_to_binary([maps:get(content_delta, D, <<>>) || D <- Deltas]).

no_secret(Term, Secret) -> ?assertEqual(nomatch, binary:match(term_to_binary(Term), Secret)).

contains(Hay, Needle) -> binary:match(Hay, Needle) =/= nomatch.

%% ---------------------------------------------------------------------------
%% compat chat

chat_complete_test() ->
    Body =
        <<"{\"model\":\"gpt-4o-mini\",\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":12,\"total_tokens\":21},\"choices\":[{\"message\":{\"content\":\"Hi there\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = cfg(openai, #{api_key => <<"sk-x">>}, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, #{
        model => <<"gpt-4o-mini">>, messages => [#{role => user, content => <<"Hello">>}]
    }),
    ?assertMatch(
        #{
            message := #{role := assistant, content := <<"Hi there">>, tool_calls := []},
            finish_reason := stop,
            usage := #{input_tokens := 9, output_tokens := 12, total_tokens := 21},
            model := <<"gpt-4o-mini">>
        },
        R
    ),
    Last = ?F:last(P),
    ?assertEqual(<<"/v1/chat/completions">>, thinwrap_llm_fake_transport:path_of(Last)),
    ?assertEqual(<<"https://api.openai.com/v1/chat/completions">>, maps:get(url, Last)),
    ?assertEqual(<<"Bearer sk-x">>, thinwrap_llm_fake_transport:header(Last, <<"Authorization">>)),
    ?assertEqual(
        <<"application/json">>, thinwrap_llm_fake_transport:header(Last, <<"Content-Type">>)
    ),
    ?assertMatch(#{<<"model">> := <<"gpt-4o-mini">>}, thinwrap_llm_fake_transport:body_json(Last)),
    ?assertNot(maps:is_key(<<"stream">>, thinwrap_llm_fake_transport:body_json(Last))).

chat_request_body_mapping_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Ok)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => <<"m">>,
        messages => [
            #{role => system, content => <<"sys">>},
            #{
                role => user,
                name => <<"bob">>,
                content_parts => [
                    #{type => text, text => <<"look">>},
                    #{type => image, base64 => <<"QUJD">>, media_type => <<"image/png">>}
                ]
            },
            #{
                role => assistant,
                tool_calls => [#{id => <<"c1">>, name => <<"f">>, arguments => <<"{}">>}]
            },
            #{role => tool, tool_call_id => <<"c1">>, content => <<"42">>}
        ],
        tools => [
            #{name => <<"f">>, description => <<"d">>, parameters => #{type => object}},
            #{name => <<"g">>}
        ],
        tool_choice => #{mode => function, function_name => <<"f">>},
        response_format => #{
            type => json_schema, json_schema_name => <<"x">>, json_schema => #{type => object}
        },
        temperature => 0.5,
        top_p => 0.9,
        stop => [<<"END">>],
        reasoning => #{effort => high}
    }),
    B = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    ?assertEqual(
        [
            #{<<"role">> => <<"system">>, <<"content">> => <<"sys">>},
            #{
                <<"role">> => <<"user">>,
                <<"name">> => <<"bob">>,
                <<"content">> => [
                    #{<<"type">> => <<"text">>, <<"text">> => <<"look">>},
                    #{
                        <<"type">> => <<"image_url">>,
                        <<"image_url">> => #{<<"url">> => <<"data:image/png;base64,QUJD">>}
                    }
                ]
            },
            #{
                <<"role">> => <<"assistant">>,
                <<"content">> => null,
                <<"tool_calls">> => [
                    #{
                        <<"id">> => <<"c1">>,
                        <<"type">> => <<"function">>,
                        <<"function">> => #{<<"name">> => <<"f">>, <<"arguments">> => <<"{}">>}
                    }
                ]
            },
            #{<<"role">> => <<"tool">>, <<"content">> => <<"42">>, <<"tool_call_id">> => <<"c1">>}
        ],
        maps:get(<<"messages">>, B)
    ),
    ?assertEqual(
        [
            #{
                <<"type">> => <<"function">>,
                <<"function">> => #{
                    <<"name">> => <<"f">>,
                    <<"description">> => <<"d">>,
                    <<"parameters">> => #{<<"type">> => <<"object">>}
                }
            },
            #{<<"type">> => <<"function">>, <<"function">> => #{<<"name">> => <<"g">>}}
        ],
        maps:get(<<"tools">>, B)
    ),
    ?assertEqual(
        #{<<"type">> => <<"function">>, <<"function">> => #{<<"name">> => <<"f">>}},
        maps:get(<<"tool_choice">>, B)
    ),
    ?assertEqual(
        #{
            <<"type">> => <<"json_schema">>,
            <<"json_schema">> => #{
                <<"name">> => <<"x">>, <<"schema">> => #{<<"type">> => <<"object">>}
            }
        },
        maps:get(<<"response_format">>, B)
    ),
    ?assertEqual(0.5, maps:get(<<"temperature">>, B)),
    ?assertEqual(0.9, maps:get(<<"top_p">>, B)),
    ?assertEqual(<<"END">>, maps:get(<<"stop">>, B)),
    ?assertEqual(<<"high">>, maps:get(<<"reasoning_effort">>, B)).

perplexity_drops_tools_and_multi_stop_is_a_list_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = cfg(perplexity, [thinwrap_llm_fake_transport:resp(200, Ok)]),
    {ok, _} = thinwrap_llm:complete(C, (one_user())#{
        tools => [#{name => <<"f">>}],
        tool_choice => #{mode => auto},
        stop => [<<"a">>, <<"b">>],
        response_format => #{type => json_object}
    }),
    B = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    ?assertNot(maps:is_key(<<"tools">>, B)),
    ?assertNot(maps:is_key(<<"tool_choice">>, B)),
    ?assertEqual([<<"a">>, <<"b">>], maps:get(<<"stop">>, B)),
    ?assertEqual(#{<<"type">> => <<"json_object">>}, maps:get(<<"response_format">>, B)),
    ?assertEqual(<<"https://api.perplexity.ai/chat/completions">>, maps:get(url, ?F:last(P))).

azure_uses_api_key_header_and_trims_base_url_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = cfg(
        'azure-openai', #{api_key => <<"az">>, base_url => <<"https://r.example/v1//">>}, [
            thinwrap_llm_fake_transport:resp(200, Ok)
        ]
    ),
    {ok, _} = thinwrap_llm:complete(C, (one_user())#{
        passthrough => #{query => #{<<"api-version">> => <<"2024-02-01">>}}
    }),
    Last = ?F:last(P),
    ?assertEqual(
        <<"https://r.example/v1/chat/completions?api-version=2024-02-01">>, maps:get(url, Last)
    ),
    ?assertEqual(<<"az">>, thinwrap_llm_fake_transport:header(Last, <<"api-key">>)),
    ?assertEqual(undefined, thinwrap_llm_fake_transport:header(Last, <<"Authorization">>)).

self_hosted_without_api_key_sends_no_auth_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = ?F:config(#{provider => ollama, headers => #{<<"X-Extra">> => <<"1">>}}, [
        thinwrap_llm_fake_transport:resp(200, Ok)
    ]),
    {ok, _} = thinwrap_llm:complete(C, one_user()),
    Last = ?F:last(P),
    ?assertEqual(<<"http://localhost:11434/v1/chat/completions">>, maps:get(url, Last)),
    ?assertEqual(undefined, thinwrap_llm_fake_transport:header(Last, <<"Authorization">>)),
    ?assertEqual(<<"1">>, thinwrap_llm_fake_transport:header(Last, <<"X-Extra">>)).

passthrough_overrides_body_headers_and_appends_query_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Ok)]),
    {ok, _} = thinwrap_llm:complete(C, (one_user())#{
        temperature => 0.1,
        passthrough => #{
            body => #{temperature => 0.9, logprobs => true},
            headers => #{<<"Authorization">> => <<"Bearer other">>, 'X-Req' => <<"y">>},
            query => #{<<"a b">> => <<"c/d*~">>}
        }
    }),
    Last = ?F:last(P),
    ?assertMatch(
        #{<<"temperature">> := 0.9, <<"logprobs">> := true},
        thinwrap_llm_fake_transport:body_json(Last)
    ),
    ?assertEqual(<<"Bearer other">>, thinwrap_llm_fake_transport:header(Last, <<"Authorization">>)),
    ?assertEqual(<<"y">>, thinwrap_llm_fake_transport:header(Last, <<"X-Req">>)),
    ?assertEqual(
        <<"https://api.openai.com/v1/chat/completions?a+b=c%2Fd%2A~">>, maps:get(url, Last)
    ).

chat_stream_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}">>,
        <<"{\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}">>,
        <<"{\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}">>,
        <<"[DONE]">>
    ]),
    {C, P} = cfg(groq, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, one_user(<<"llama">>)),
    ?assertEqual(<<"Hello">>, text(Ds)),
    ?assertEqual([stop], [F || #{finish_reason := F} <- Ds]),
    ?assertEqual(
        [#{input_tokens => 3, output_tokens => 2, total_tokens => 5}], [U || #{usage := U} <- Ds]
    ),
    Last = ?F:last(P),
    ?assertMatch(
        #{<<"stream">> := true, <<"stream_options">> := #{<<"include_usage">> := true}},
        thinwrap_llm_fake_transport:body_json(Last)
    ),
    ?assertEqual(<<"text/event-stream">>, thinwrap_llm_fake_transport:header(Last, <<"Accept">>)),
    %% every delta carries the vendor chunk it came from
    ?assert(lists:all(fun(D) -> is_map(maps:get(raw, D)) end, Ds)).

groq_usage_under_x_groq_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"x_groq\":{\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}}">>,
        <<"[DONE]">>
    ]),
    {C, _} = cfg(groq, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, one_user()),
    ?assertEqual(
        [#{input_tokens => 1, output_tokens => 1, total_tokens => 2}], [U || #{usage := U} <- Ds]
    ).

mistral_stream_has_no_stream_options_test() ->
    {C, P} = cfg(mistral, [
        thinwrap_llm_fake_transport:resp(200, thinwrap_llm_fake_transport:sse([<<"[DONE]">>]), ?SSE)
    ]),
    {ok, []} = collect(C, one_user()),
    B = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    ?assertEqual(true, maps:get(<<"stream">>, B)),
    ?assertNot(maps:is_key(<<"stream_options">>, B)).

error_mapping_test() ->
    {C, _} = cfg(openai, [
        thinwrap_llm_fake_transport:resp(429, <<"{\"error\":{\"message\":\"slow down\"}}">>)
    ]),
    ?assertMatch(
        {error, #{code := rate_limited, status := 429, provider_message := <<"slow down">>}},
        thinwrap_llm:complete(C, one_user())
    ).

context_length_error_test() ->
    {C, _} = cfg(openai, [
        thinwrap_llm_fake_transport:resp(
            400,
            <<"{\"error\":{\"message\":\"This model maximum context length is 8192 tokens\"}}">>
        )
    ]),
    ?assertMatch(
        {error, #{code := context_length_exceeded}}, thinwrap_llm:complete(C, one_user())
    ).

together_eos_finish_reason_maps_to_stop_test() ->
    Body =
        <<"{\"model\":\"m\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2},\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"eos\"}]}">>,
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    ?assertMatch({ok, #{finish_reason := stop}}, thinwrap_llm:complete(C, one_user())).

usage_total_falls_back_to_sum_test() ->
    Body =
        <<"{\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":3},\"choices\":[{\"message\":{\"content\":\"ok\",\"tool_calls\":[{\"id\":\"c\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"function_call\"}]}">>,
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, one_user(<<"req-model">>)),
    ?assertMatch(
        #{
            usage := #{total_tokens := 7},
            finish_reason := tool_calls,
            model := <<"req-model">>,
            message := #{tool_calls := [#{id := <<"c">>, name := <<"f">>, arguments := <<"{}">>}]}
        },
        R
    ).

stream_error_frame_raises_mid_stream_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}">>,
        <<"{\"error\":{\"message\":\"boom mid-stream\"}}">>
    ]),
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {Delivered, Result} = collect_until_error(C, one_user()),
    ?assertEqual(<<"Hi">>, text(Delivered)),
    ?assertMatch({error, #{provider_message := <<"boom mid-stream">>}}, Result).

stream_multiple_tool_calls_in_one_chunk_test() ->
    Chunk =
        <<"{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}},{\"index\":1,\"id\":\"call_b\",\"function\":{\"name\":\"g\",\"arguments\":\"{}\"}}]}}]}">>,
    {C, _} = cfg(openai, [
        thinwrap_llm_fake_transport:resp(
            200, thinwrap_llm_fake_transport:sse([Chunk, <<"[DONE]">>]), ?SSE
        )
    ]),
    {ok, Ds} = collect(C, one_user()),
    ?assertEqual(
        [{0, <<"call_a">>, <<"f">>}, {1, <<"call_b">>, <<"g">>}],
        [{I, Id, N} || #{tool_call_delta := #{index := I, id := Id, function_name := N}} <- Ds]
    ).

%% ---------------------------------------------------------------------------
%% transport failures

transport_error_does_not_leak_credentials_test() ->
    {C, _} = cfg(openai, [
        {error, {econnreset, "reset while calling https://api/v1?api_key=SUPERSECRET"}}
    ]),
    {error, E} = thinwrap_llm:complete(C, one_user()),
    ?assertMatch(#{code := provider_unavailable}, E),
    ?assertNot(maps:is_key(status, E)),
    ?assertEqual(<<"Upstream transport error (econnreset)">>, maps:get(message, E)),
    ?assertEqual(#{<<"raw">> => #{<<"exception">> => <<"econnreset">>}}, maps:get(cause, E)),
    no_secret(E, <<"SUPERSECRET">>).

transport_failure_without_a_status_stays_provider_unavailable_test() ->
    {C, _} = cfg(openai, [{error, closed}]),
    {error, E} = thinwrap_llm:complete(C, one_user()),
    ?assertMatch(#{code := provider_unavailable}, E),
    ?assertNot(maps:is_key(status, E)).

stream_connect_failure_is_provider_unavailable_test() ->
    {C, _} = cfg(openai, [{error, {failed_connect, [{to_address, {"h", 443}}]}}]),
    ?assertMatch(
        {error, #{
            code := provider_unavailable,
            message := <<"Upstream transport error (failed_connect)">>
        }},
        collect(C, one_user())
    ).

mid_stream_transport_read_error_becomes_connector_error_test() ->
    Body = thinwrap_llm_fake_transport:sse([<<"{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}">>]),
    {C, _} = cfg(openai, [
        {stream_error, thinwrap_llm_fake_transport:resp(200, Body, ?SSE),
            {closed, "https://h/v1?token=LEAKME"}}
    ]),
    {Delivered, Result} = collect_until_error(C, one_user()),
    ?assertEqual(<<"Hi">>, text(Delivered)),
    ?assertMatch({error, #{code := provider_unavailable}}, Result),
    no_secret(Result, <<"LEAKME">>).

%% A transport that crashes, breaks its return contract, or refuses up front.
request(_Req, crash) ->
    erlang:error({boom, <<"https://h/x?key=supersecret">>});
request(_Req, bad_return) ->
    {ok, not_a_map};
request(_Req, typed) ->
    {error, {connector_error, thinwrap_llm_error:invalid_request(<<"nope">>)}};
request(_Req, {buffered, Resp}) ->
    {ok, Resp}.

crashing_transport_is_provider_unavailable_test() ->
    C = #{provider => openai, api_key => <<"k">>, transport => {?MODULE, crash}},
    {error, E} = thinwrap_llm:complete(C, one_user()),
    ?assertMatch(
        #{code := provider_unavailable, message := <<"Upstream transport error (boom)">>}, E
    ),
    no_secret(E, <<"supersecret">>),
    ?assertMatch({error, #{code := provider_unavailable}}, collect(C, one_user())).

contract_breaking_transport_is_an_error_not_an_outage_test() ->
    C = #{provider => openai, api_key => <<"k">>, transport => {?MODULE, bad_return}},
    ?assertError({bad_transport_return, ?MODULE, _}, thinwrap_llm:complete(C, one_user())),
    ?assertError({bad_transport_return, ?MODULE, _}, collect(C, one_user())).

typed_transport_error_passes_through_test() ->
    C = #{provider => openai, api_key => <<"k">>, transport => {?MODULE, typed}},
    ?assertMatch(
        {error, #{code := invalid_request, message := <<"nope">>}},
        thinwrap_llm:complete(C, one_user())
    ),
    ?assertMatch({error, #{code := invalid_request}}, collect(C, one_user())).

buffered_stream_fallback_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}">>,
        <<"{\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}">>,
        <<"[DONE]">>
    ]),
    C = #{
        provider => openai,
        api_key => <<"k">>,
        transport => {?MODULE, {buffered, thinwrap_llm_fake_transport:resp(200, Body)}}
    },
    {ok, Ds} = collect(C, one_user()),
    ?assertEqual(<<"hi">>, text(Ds)),
    E = maps:remove(body, thinwrap_llm_fake_transport:resp(401, <<>>)),
    C2 = C#{transport => {?MODULE, {buffered, E#{body => <<"{\"error\":{\"message\":\"no\"}}">>}}}},
    ?assertMatch({error, #{code := auth_failed, status := 401}}, collect(C2, one_user())).

user_fun_exceptions_propagate_unchanged_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}">>, <<"[DONE]">>
    ]),
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    ?assertError(
        my_bug, thinwrap_llm:stream(C, one_user(), fun(_, _) -> erlang:error(my_bug) end, ok)
    ),
    %% A caller's fold may end the stream with a throw; it must leave the
    %% library unchanged too. raise/3 produces exactly the `throw` class a
    %% throw/1 would.
    ?assertThrow(
        stop_now,
        thinwrap_llm:stream(C, one_user(), fun(_, _) -> erlang:raise(throw, stop_now, []) end, ok)
    ).

%% ---------------------------------------------------------------------------
%% embeddings

embeddings_test() ->
    Body =
        <<"{\"model\":\"text-embedding-3-small\",\"data\":[{\"index\":1,\"embedding\":[0.3,0.4]},{\"index\":0,\"embedding\":[0.1,2]}],\"usage\":{\"prompt_tokens\":5,\"total_tokens\":5}}">>,
    {C, P} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:embed(C, #{
        model => <<"text-embedding-3-small">>, input => [<<"a">>, <<"b">>]
    }),
    ?assertMatch(
        #{
            embeddings := [[0.1, 2.0], [0.3, 0.4]],
            usage := #{input_tokens := 5, total_tokens := 5},
            model := <<"text-embedding-3-small">>
        },
        R
    ),
    Last = ?F:last(P),
    ?assertEqual(<<"https://api.openai.com/v1/embeddings">>, maps:get(url, Last)),
    ?assertEqual(
        #{
            <<"model">> => <<"text-embedding-3-small">>,
            <<"input">> => [<<"a">>, <<"b">>],
            <<"encoding_format">> => <<"float">>
        },
        thinwrap_llm_fake_transport:body_json(Last)
    ).

embeddings_unsupported_is_badarg_test() ->
    ?assertError(
        {badarg, _},
        thinwrap_llm:embed(#{provider => deepseek, api_key => <<"k">>}, #{
            model => <<"m">>, input => [<<"a">>]
        })
    ),
    ?assertError(
        {badarg, _},
        thinwrap_llm:embed(#{provider => anthropic, api_key => <<"k">>}, #{
            model => <<"m">>, input => [<<"a">>]
        })
    ).

embeddings_passthrough_query_test() ->
    Body =
        <<"{\"model\":\"m\",\"data\":[{\"index\":0,\"embedding\":[0.1]}],\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}">>,
    {C, P} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, _} = thinwrap_llm:embed(C, #{
        model => <<"m">>,
        input => [<<"a">>],
        passthrough => #{query => #{<<"api-version">> => <<"2024-02-01">>}}
    }),
    ?assertEqual(
        <<"https://api.openai.com/v1/embeddings?api-version=2024-02-01">>, maps:get(url, ?F:last(P))
    ).

mistral_embeddings_send_output_dimension_test() ->
    Body =
        <<"{\"model\":\"codestral-embed\",\"data\":[{\"index\":0,\"embedding\":[0.1,0.2]}],\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}">>,
    {C, P} = cfg(mistral, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, _} = thinwrap_llm:embed(C, #{
        model => <<"codestral-embed">>, input => [<<"a">>], dimensions => 256
    }),
    B = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    ?assertEqual(256, maps:get(<<"output_dimension">>, B)),
    ?assertNot(maps:is_key(<<"dimensions">>, B)),
    {C2, P2} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, _} = thinwrap_llm:embed(C2, #{model => <<"m">>, input => [<<"a">>], dimensions => 256}),
    ?assertEqual(
        256, maps:get(<<"dimensions">>, thinwrap_llm_fake_transport:body_json(?F:last(P2)))
    ).

emb(Body) ->
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    thinwrap_llm:embed(C, #{model => <<"m">>, input => [<<"a">>]}).

embeddings_200_error_object_raises_mapped_error_test() ->
    ?assertMatch(
        {error, #{
            code := provider_unavailable,
            status := 200,
            provider_message := <<"No provider available">>
        }},
        emb(<<"{\"error\":{\"code\":503,\"message\":\"No provider available\"}}">>)
    ).

embeddings_200_error_object_without_usable_code_is_unknown_test() ->
    ?assertMatch(
        {error, #{code := unknown, status := 200}}, emb(<<"{\"error\":{\"message\":\"odd\"}}">>)
    ).

embeddings_in_body_string_code_classifies_like_chat_test() ->
    ?assertMatch(
        {error, #{code := rate_limited, status := 200}},
        emb(
            <<"{\"error\":{\"code\":\"insufficient_quota\",\"message\":\"You exceeded your current quota\"}}">>
        )
    ).

embeddings_200_without_data_is_empty_body_error_test_() ->
    [
        ?_assertMatch({error, #{code := provider_unavailable, status := 200}}, emb(B))
     || B <- [<<"{}">>, <<"{\"data\":null,\"model\":\"m\"}">>, <<>>, <<"<html>">>]
    ].

embeddings_count_mismatch_raises_test() ->
    Body =
        <<"{\"model\":\"m\",\"data\":[{\"index\":0,\"embedding\":[0.1,0.2]}],\"usage\":{\"prompt_tokens\":2,\"total_tokens\":2}}">>,
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {error, E} = thinwrap_llm:embed(C, #{model => <<"m">>, input => [<<"a">>, <<"b">>]}),
    ?assertMatch(
        #{
            code := provider_unavailable,
            message := <<"provider returned 1 embeddings for 2 inputs">>,
            cause := #{<<"raw">> := #{<<"model">> := <<"m">>}}
        },
        E
    ).

embeddings_non_numeric_cell_raises_test_() ->
    [
        ?_assertMatch(
            {error, #{code := provider_unavailable}},
            emb(
                <<"{\"model\":\"m\",\"data\":[{\"index\":0,\"embedding\":[0.1,", Cell/binary,
                    "]}]}">>
            )
        )
     || Cell <- [<<"null">>, <<"\"0.5\"">>, <<"true">>]
    ].

embeddings_row_without_vector_raises_test() ->
    ?assertMatch(
        {error, #{code := provider_unavailable}},
        emb(<<"{\"model\":\"m\",\"data\":[{\"index\":0}]}">>)
    ).

embeddings_status_classification_test_() ->
    [
        ?_test(begin
            {C, _} = cfg(openai, [
                thinwrap_llm_fake_transport:resp(S, <<"{\"error\":{\"message\":\"nope\"}}">>)
            ]),
            ?assertMatch(
                {error, #{code := Want, status := S}},
                thinwrap_llm:embed(C, #{model => <<"m">>, input => [<<"a">>]})
            )
        end)
     || {S, Want} <- [
            {401, auth_failed},
            {402, rate_limited},
            {408, provider_unavailable},
            {409, invalid_request},
            {413, invalid_request},
            {503, provider_unavailable}
        ]
    ].

%% ---------------------------------------------------------------------------
%% chunk-array content (Mistral reasoning)

complete_with_chunk_array_content_keeps_only_text_test() ->
    Body =
        <<"{\"model\":\"magistral-medium-latest\",\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":9,\"total_tokens\":14},\"choices\":[{\"index\":0,\"finish_reason\":\"stop\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"thinking\",\"thinking\":[{\"type\":\"text\",\"text\":\"Let me work this out.\"}]},{\"type\":\"text\",\"text\":\"The answer \"},{\"type\":\"text\",\"text\":\"is 4.\"}]}}]}">>,
    {C, _} = cfg(mistral, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, one_user()),
    ?assertMatch(#{message := #{content := <<"The answer is 4.">>}}, R),
    #{raw := #{<<"choices">> := [#{<<"message">> := #{<<"content">> := [First | _]}}]}} = R,
    ?assertMatch(#{<<"type">> := <<"thinking">>}, First).

complete_with_only_thinking_chunks_has_empty_content_test() ->
    Body =
        <<"{\"choices\":[{\"finish_reason\":\"length\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"thinking\",\"thinking\":[{\"type\":\"text\",\"text\":\"hmm\"}]}]}}]}">>,
    {C, _} = cfg(mistral, [thinwrap_llm_fake_transport:resp(200, Body)]),
    ?assertMatch({ok, #{message := #{content := <<>>}}}, thinwrap_llm:complete(C, one_user())).

stream_with_chunk_array_deltas_emits_only_answer_text_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":[{\"type\":\"thinking\",\"thinking\":[{\"type\":\"text\",\"text\":\"Considering\"}]}]}}]}">>,
        <<"{\"choices\":[{\"index\":0,\"delta\":{\"content\":[{\"type\":\"thinking\",\"thinking\":[{\"type\":\"text\",\"text\":\" done.\"}]},{\"type\":\"text\",\"text\":\"The answer\"}]}}]}">>,
        <<"{\"choices\":[{\"index\":0,\"delta\":{\"content\":\" is 4.\"}}]}">>,
        <<"{\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}]}">>,
        <<"[DONE]">>
    ]),
    {C, _} = cfg(mistral, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, one_user()),
    ?assertEqual(<<"The answer is 4.">>, text(Ds)),
    ?assertEqual([stop], [F || #{finish_reason := F} <- Ds]).

%% ---------------------------------------------------------------------------
%% 2xx bodies that are not a result (OpenRouter)

openrouter(Body) ->
    {C, _} = cfg(openrouter, [thinwrap_llm_fake_transport:resp(200, Body)]),
    thinwrap_llm:complete(C, one_user()).

openrouter_200_with_only_error_object_raises_mapped_error_test() ->
    {error, E} = openrouter(
        <<"{\"error\":{\"code\":502,\"message\":\"Provider returned error\"}}">>
    ),
    ?assertMatch(
        #{
            code := provider_unavailable,
            status := 200,
            provider_message := <<"Provider returned error">>,
            cause := #{<<"raw">> := #{<<"error">> := #{<<"code">> := 502}}}
        },
        E
    ).

openrouter_200_error_code_429_maps_to_rate_limited_test() ->
    ?assertMatch(
        {error, #{code := rate_limited, status := 200}},
        openrouter(<<"{\"error\":{\"code\":429,\"message\":\"Rate limit exceeded upstream\"}}">>)
    ).

two_hundred_error_object_with_string_code_classifies_by_name_test() ->
    ?assertMatch(
        {error, #{code := provider_unavailable, status := 200}},
        openrouter(<<"{\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}">>)
    ).

openrouter_200_with_choice_finish_reason_error_raises_test() ->
    ?assertMatch(
        {error, #{
            code := provider_unavailable,
            status := 200,
            provider_message := <<"Upstream disconnected">>
        }},
        openrouter(
            <<"{\"id\":\"gen-1\",\"model\":\"openai/gpt-4o\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"Partial answ\"},\"finish_reason\":\"error\",\"error\":{\"code\":502,\"message\":\"Upstream disconnected\"}}]}">>
        )
    ).

choice_finish_reason_error_without_error_object_raises_test() ->
    ?assertMatch(
        {error, #{code := unknown, status := 200, message := <<"HTTP 200">>}},
        openrouter(
            <<"{\"choices\":[{\"message\":{\"content\":\"x\"},\"finish_reason\":\"error\"}]}">>
        )
    ).

two_hundred_error_object_without_usable_code_is_unknown_test() ->
    ?assertMatch(
        {error, #{code := unknown, status := 200}},
        openrouter(<<"{\"error\":{\"message\":\"something failed\"}}">>)
    ).

chat_in_body_string_code_classifies_test() ->
    ?assertMatch(
        {error, #{code := rate_limited, status := 200}},
        openrouter(
            <<"{\"error\":{\"code\":\"insufficient_quota\",\"message\":\"You exceeded your current quota\"}}">>
        )
    ).

compat_200_without_choices_is_empty_body_error_test_() ->
    [
        ?_assertMatch({error, #{code := provider_unavailable, status := 200}}, openrouter(B))
     || B <- [
            <<"{}">>,
            <<"{\"choices\":[]}">>,
            <<"{\"choices\":null}">>,
            <<"{\"id\":\"x\",\"model\":\"m\"}">>,
            <<"not json">>
        ]
    ].

%% ---------------------------------------------------------------------------
%% max-tokens field per provider

openai_and_azure_send_max_completion_tokens_test_() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    [
        ?_test(begin
            {C, P} = cfg(Provider, Extra, [thinwrap_llm_fake_transport:resp(200, Ok)]),
            {ok, _} = thinwrap_llm:complete(C, (one_user(<<"o4-mini">>))#{max_output_tokens => 256}),
            B = thinwrap_llm_fake_transport:body_json(?F:last(P)),
            ?assertEqual(256, maps:get(<<"max_completion_tokens">>, B)),
            ?assertNot(maps:is_key(<<"max_tokens">>, B))
        end)
     || {Provider, Extra} <- [
            {openai, #{api_key => <<"k">>}},
            {'azure-openai', #{
                api_key => <<"k">>, base_url => <<"https://res.openai.azure.com/openai/v1">>
            }}
        ]
    ].

other_compat_providers_keep_max_tokens_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = cfg(groq, [thinwrap_llm_fake_transport:resp(200, Ok)]),
    {ok, _} = thinwrap_llm:complete(C, (one_user(<<"llama">>))#{max_output_tokens => 64}),
    B = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    ?assertEqual(64, maps:get(<<"max_tokens">>, B)),
    ?assertNot(maps:is_key(<<"max_completion_tokens">>, B)).

%% ---------------------------------------------------------------------------
%% status classification, Retry-After

compat_status_classification_test_() ->
    [
        ?_test(begin
            {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(S, B)]),
            ?assertMatch(
                {error, #{code := Want, status := S}}, thinwrap_llm:complete(C, one_user())
            )
        end)
     || {S, B, Want} <- [
            {401, <<"{\"error\":{\"message\":\"bad key\"}}">>, auth_failed},
            {402, <<"{\"error\":{\"message\":\"Insufficient credits\"}}">>, rate_limited},
            {408, <<"{\"error\":{\"message\":\"timeout\"}}">>, provider_unavailable},
            {409, <<"{\"error\":{\"message\":\"conflict\"}}">>, invalid_request},
            {410, <<"{\"error\":{\"message\":\"gone\"}}">>, invalid_request},
            {413, <<"{\"error\":{\"message\":\"Insufficient credits\"}}">>, invalid_request},
            {503, <<"oops">>, provider_unavailable},
            {400,
                <<"{\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"Too many tokens for this model.\"}}">>,
                context_length_exceeded},
            {400,
                <<"{\"error\":{\"code\":\"content_policy_violation\",\"message\":\"Your request was rejected as a result of our safety system: content_policy_violation\"}}">>,
                content_filtered},
            {400, <<"{\"error\":{\"message\":\"Flagged by moderation\"}}">>, content_filtered},
            {400,
                <<"{\"error\":{\"message\":\"The response was filtered due to the prompt triggering content filter policy\"}}">>,
                content_filtered},
            {400, <<"{\"error\":{\"message\":\"Invalid value for temperature\"}}">>,
                invalid_request}
        ]
    ].

non_json_error_body_message_is_http_status_test() ->
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(502, <<"<html>bad gateway</html>">>)]),
    {error, E} = thinwrap_llm:complete(C, one_user()),
    ?assertMatch(
        #{code := provider_unavailable, message := <<"HTTP 502">>, cause := #{<<"raw">> := null}}, E
    ).

retry(Value) ->
    {C, _} = cfg(openai, [
        thinwrap_llm_fake_transport:resp(429, <<"{\"error\":{\"message\":\"slow\"}}">>, #{
            <<"Retry-After">> => Value
        })
    ]),
    {error, #{cause := Cause}} = thinwrap_llm:complete(C, one_user()),
    Cause.

unusable_retry_after_has_no_seconds_test_() ->
    [
        ?_test(begin
            Cause = retry(V),
            ?assertNot(maps:is_key(<<"retryAfterSeconds">>, Cause)),
            case string:trim(V) of
                <<>> -> ?assertNot(maps:is_key(<<"retryAfter">>, Cause));
                T -> ?assertEqual(T, maps:get(<<"retryAfter">>, Cause))
            end
        end)
     || V <- [
            <<>>,
            <<"   ">>,
            <<"-5">>,
            <<"1.5">>,
            <<"+5">>,
            <<"soon">>,
            <<"tomorrow">>,
            <<"2015-10-21T07:28:00Z">>,
            <<"Wed, 21 Oct 2015 07:28:00 +0000">>,
            <<"Wed, 31 Feb 2015 07:28:00 GMT">>
        ]
    ].

future_retry_after_date_is_parsed_test() ->
    When = erlang:system_time(second) + 120,
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(When, second),
    Days = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"},
    Months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"},
    Date = iolist_to_binary(
        io_lib:format("~s, ~2..0B ~s ~4..0B ~2..0B:~2..0B:~2..0B GMT", [
            element(calendar:day_of_the_week(Y, Mo, D), Days),
            D,
            element(Mo, Months),
            Y,
            H,
            Mi,
            S
        ])
    ),
    Secs = maps:get(<<"retryAfterSeconds">>, retry(Date)),
    ?assert(Secs >= 100 andalso Secs =< 120).

past_http_date_retry_after_means_retry_now_test_() ->
    [
        ?_assertEqual(0, maps:get(<<"retryAfterSeconds">>, retry(V)))
     || V <- [
            <<"Wed, 21 Oct 2015 07:28:00 GMT">>,
            <<"Wednesday, 21-Oct-15 07:28:00 GMT">>,
            <<"Wed Oct 21 07:28:00 2015">>
        ]
    ].

retry_after_integer_seconds_test() ->
    ?assertMatch(
        #{<<"retryAfter">> := <<"30">>, <<"retryAfterSeconds">> := 30, <<"raw">> := #{}},
        retry(<<"30">>)
    ).

rfc850_two_digit_year_far_ahead_is_last_century_test() ->
    %% 2026 + 50 < 2099: "99" means 1999, already past.
    Now = calendar:datetime_to_gregorian_seconds({{2026, 11, 1}, {0, 0, 0}}) - 62167219200,
    ?assertEqual(
        0, thinwrap_llm_error:parse_retry_after(<<"Friday, 31-Dec-99 23:59:59 GMT">>, Now)
    ),
    ?assertEqual(
        60, thinwrap_llm_error:parse_retry_after(<<"Sunday, 01-Nov-26 00:01:00 GMT">>, Now)
    ).

%% ---------------------------------------------------------------------------
%% mid-stream error frames

mid_stream_error_honours_integer_code_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}">>,
        <<"{\"error\":{\"code\":429,\"message\":\"Upstream busy\"},\"choices\":[{\"delta\":{},\"finish_reason\":\"error\"}]}">>
    ]),
    {C, _} = cfg(openrouter, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    ?assertMatch({error, #{code := rate_limited}}, collect(C, one_user())).

compat_mid_stream_frame_classified_by_code_or_type_test_() ->
    [
        ?_test(begin
            Body = thinwrap_llm_fake_transport:sse([
                <<"{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}">>, Frame
            ]),
            {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
            ?assertMatch({error, #{code := Want, status := 200}}, collect(C, one_user()))
        end)
     || {Frame, Want} <- [
            {<<"{\"error\":{\"message\":\"something broke\"}}">>, unknown},
            {<<"{\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"slow\"}}">>,
                rate_limited},
            {<<"{\"error\":{\"code\":\"502\",\"message\":\"upstream\"}}">>, provider_unavailable}
        ]
    ].

%% ---------------------------------------------------------------------------
%% stream termination: non-SSE bodies, truncated streams, error bodies

stream_factories() ->
    [
        {openai, #{api_key => <<"k">>}},
        {anthropic, #{api_key => <<"k">>}},
        {gemini, #{api_key => <<"k">>}}
    ].

stream_error_body_read_failure_is_guarded_test_() ->
    [
        ?_test(begin
            {C, _} = cfg(Provider, Extra, [
                {stream_error, thinwrap_llm_fake_transport:resp(429, <<"{\"error\":{\"mess">>),
                    {closed, "https://host/v1?key=LEAKME"}}
            ]),
            {error, E} = collect(C, one_user()),
            ?assertMatch(#{code := rate_limited, status := 429}, E),
            no_secret(E, <<"LEAKME">>)
        end)
     || {Provider, Extra} <- stream_factories()
    ].

stream_non_2xx_is_mapped_test_() ->
    [
        ?_assertMatch(
            {error, #{code := auth_failed, status := 401}},
            begin
                {C, _} = cfg(Provider, Extra, [
                    thinwrap_llm_fake_transport:resp(
                        401, <<"{\"error\":{\"message\":\"Invalid API key\"}}">>
                    )
                ]),
                collect(C, one_user())
            end
        )
     || {Provider, Extra} <- stream_factories()
    ].

two_xx_stream_with_non_sse_content_type_raises_test_() ->
    [
        ?_test(begin
            {C, _} = cfg(Provider, Extra, [
                thinwrap_llm_fake_transport:resp(200, <<"{\"choices\":[]}">>, #{
                    <<"content-type">> => CT
                })
            ]),
            {error, E} = collect(C, one_user()),
            ?assertMatch(#{code := provider_unavailable, status := 200}, E),
            [Media | _] = binary:split(CT, <<";">>),
            ?assert(contains(maps:get(message, E), Media))
        end)
     || {Provider, Extra} <- stream_factories(),
        CT <- [<<"application/json">>, <<"text/html; charset=utf-8">>]
    ].

sse_or_missing_content_type_is_accepted_test_() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":\"stop\"}]}">>,
        <<"[DONE]">>
    ]),
    [
        ?_test(begin
            {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, H)]),
            {ok, [First | _]} = collect(C, one_user()),
            ?assertEqual(<<"Hi">>, maps:get(content_delta, First))
        end)
     || H <- [
            #{},
            #{<<"content-type">> => <<"text/event-stream; charset=utf-8">>},
            #{<<"content-type">> => <<"TEXT/EVENT-STREAM">>}
        ]
    ].

compat_stream_with_terminal_event_completes_test_() ->
    [
        ?_test(begin
            {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
            {ok, Ds} = collect(C, one_user()),
            ?assertEqual(<<"Hi">>, text(Ds))
        end)
     || Body <- [
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\ndata: [DONE]\n\n">>,
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":\"stop\"}]}\n\n">>,
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\ndata: [DONE]">>,
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\r\n\r\ndata: [DONE]\r\n\r\n">>,
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\r\rdata: [DONE]\r\r">>,
            %% anything after [DONE] is never read
            <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\ndata: [DONE]\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"X\"}}]}\n\n">>
        ]
    ].

compat_stream_truncated_before_terminal_event_raises_test() ->
    Body = <<"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n">>,
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {Delivered, Result} = collect_until_error(C, one_user()),
    ?assertEqual(<<"Hi">>, text(Delivered)),
    {error, E} = Result,
    ?assertMatch(#{code := provider_unavailable}, E),
    ?assert(contains(maps:get(message, E), <<"terminal event">>)).

compat_empty_stream_body_raises_test() ->
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, <<>>, ?SSE)]),
    ?assertMatch({error, #{code := provider_unavailable}}, collect(C, one_user())).

stream_keepalive_comments_and_bad_json_are_skipped_test() ->
    Body = <<
        ": keep-alive\n\nevent: message\nid: 1\ndata: not json\n\n"
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":\"stop\"}]}\n\n"
    >>,
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, one_user()),
    ?assertEqual(<<"Hi">>, text(Ds)).

anthropic_stream_truncated_before_stop_raises_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}">>,
        <<"{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}">>,
        <<"{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}">>
    ]),
    {C, _} = cfg(anthropic, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {Delivered, Result} = collect_until_error(C, one_user()),
    ?assertEqual(<<"Hi">>, text(Delivered)),
    ?assertMatch({error, #{code := provider_unavailable}}, Result).

anthropic_stream_ending_on_message_stop_completes_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}">>,
        <<"{\"type\":\"message_stop\"}">>
    ]),
    {C, _} = cfg(anthropic, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, one_user()),
    ?assertEqual([<<"Hi">>], [maps:get(content_delta, D, <<>>) || D <- Ds]).

gemini_stream_truncated_before_finish_reason_raises_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hi\"}]}}],\"usageMetadata\":{\"promptTokenCount\":3}}">>
    ]),
    {C, _} = cfg(gemini, [thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {Delivered, Result} = collect_until_error(C, one_user()),
    ?assertMatch([#{content_delta := <<"Hi">>}, #{usage := #{input_tokens := 3}}], Delivered),
    ?assertMatch({error, #{code := provider_unavailable}}, Result).

bedrock_stream_without_output_raises_test() ->
    {C, _} = cfg(
        bedrock,
        #{region => <<"us-east-1">>, access_key_id => <<"a">>, secret_access_key => <<"s">>},
        [
            thinwrap_llm_fake_transport:resp(
                200, <<"{\"stopReason\":\"end_turn\",\"usage\":{\"inputTokens\":1}}">>
            )
        ]
    ),
    ?assertMatch({error, #{code := provider_unavailable}}, collect(C, one_user())).

%% ---------------------------------------------------------------------------
%% default transport refusals, header values

default_transport_refuses_http_local_defaults_test_() ->
    [
        ?_test(begin
            C = #{provider => Provider},
            [
                begin
                    {error, E} = Call(),
                    ?assertMatch(#{code := invalid_request}, E),
                    M = maps:get(message, E),
                    ?assert(contains(M, <<"http://">>)),
                    ?assert(contains(M, <<"transport">>)),
                    ?assertNot(contains(M, <<"localhost">>))
                end
             || Call <- [
                    fun() -> thinwrap_llm:complete(C, one_user()) end,
                    fun() -> collect(C, one_user()) end
                ]
            ]
        end)
     || Provider <- [vllm, ollama, lmstudio]
    ].

default_transport_refuses_file_scheme_test() ->
    ?assertMatch(
        {error, {connector_error, #{code := invalid_request}}},
        thinwrap_llm_httpc:request(
            #{method => get, url => <<"file:///etc/passwd">>, headers => #{}}, #{}
        )
    ).

header_with_line_break_is_invalid_request_without_the_value_test_() ->
    [
        ?_test(begin
            {C, P} = ?F:config(#{provider => openai, api_key => <<"sk-SECRET\n">>}, [
                thinwrap_llm_fake_transport:resp(200, <<"{}">>)
            ]),
            {error, E} = Call(C),
            ?assertMatch(
                #{code := invalid_request, provider_message := <<"Invalid header value">>}, E
            ),
            ?assert(contains(maps:get(message, E), <<"Authorization">>)),
            no_secret(E, <<"SECRET">>),
            ?assertEqual([], ?F:calls(P))
        end)
     || Call <- [
            fun(C) -> thinwrap_llm:complete(C, one_user()) end,
            fun(C) -> collect(C, one_user()) end
        ]
    ].

header_name_with_colon_is_refused_test() ->
    {C, _} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, <<"{}">>)]),
    {error, E} = thinwrap_llm:complete(C, (one_user())#{
        passthrough => #{headers => #{<<"X-Bad:\x01">> => <<"v">>}}
    }),
    ?assertEqual(
        <<"Header 'X-Bad:?' contains a line break or NUL character; check the configured credential and headers">>,
        maps:get(message, E)
    ).

%% ---------------------------------------------------------------------------
%% SSE parser

sse_all(Chunks) -> sse_all(Chunks, thinwrap_llm_sse:new()).

sse_all(Chunks, S0) ->
    {Out, S1} = lists:foldl(
        fun(C, {Acc, S}) ->
            {Ps, S2} = thinwrap_llm_sse:feed(C, S),
            {Acc ++ Ps, S2}
        end,
        {[], S0},
        Chunks
    ),
    {Tail, S3} = thinwrap_llm_sse:finish(S1),
    {Out ++ Tail, thinwrap_llm_sse:done(S3)}.

parse_sse_lines_test() ->
    Body = <<"data: {\"a\":1}\n\n: comment\ndata: {\"b\":2}\n\ndata: [DONE]\n\n">>,
    ?assertEqual({[<<"{\"a\":1}">>, <<"{\"b\":2}">>], true}, sse_all([Body])).

sse_data_strips_exactly_one_leading_space_test() ->
    ?assertEqual({[<<"{\"a\":1}">>], false}, sse_all([<<"data:{\"a\":1}\n\n">>])),
    ?assertEqual({[<<"{\"a\":1}">>], false}, sse_all([<<"data: {\"a\":1}\n\n">>])),
    ?assertEqual({[<<" {\"a\":1}">>], false}, sse_all([<<"data:  {\"a\":1}\n\n">>])).

sse_multi_line_data_is_joined_test() ->
    ?assertEqual({[<<"a\nb">>], false}, sse_all([<<"data: a\r\ndata: b\r\n\r\n">>])).

%% Every way of cutting the body must give the same payloads.
sse_every_split_point_gives_the_same_payloads_test() ->
    Body = <<
        "event: x\r\ndata: one\r\ndata: two\r\n\r\n: c\rdata: \xe2\x80\xa8three\r\rdata: four\n\n"
        "data: [DONE]\n\ndata: never\n\n"
    >>,
    Want = {[<<"one\ntwo">>, <<"\xe2\x80\xa8three">>, <<"four">>], true},
    ?assertEqual(Want, sse_all([Body])),
    [
        ?assertEqual(
            Want,
            sse_all([binary:part(Body, 0, I), binary:part(Body, I, byte_size(Body) - I)]),
            I
        )
     || I <- lists:seq(1, byte_size(Body) - 1)
    ],
    ?assertEqual(Want, sse_all([<<B>> || <<B>> <= Body])).

sse_final_event_without_blank_line_is_dispatched_test() ->
    ?assertEqual({[<<"x">>], false}, sse_all([<<"data: x">>])),
    ?assertEqual({[], true}, sse_all([<<"data: [DONE]">>])),
    ?assertEqual({[<<"x">>], false}, sse_all([<<"data: x\n">>])).

sse_caps_line_and_event_size_test() ->
    ?assertThrow(
        {thinwrap_llm_error, #{
            code := provider_unavailable, message := <<"SSE line exceeds the 16 MiB cap">>
        }},
        sse_all([binary:copy(<<"x">>, 40)], thinwrap_llm_sse:new(8))
    ),
    ?assertThrow(
        {thinwrap_llm_error, #{message := <<"SSE event exceeds the 16 MiB cap">>}},
        sse_all([<<"data: abcd\ndata: efgh\n">>], thinwrap_llm_sse:new(8))
    ),
    ?assertEqual(
        {[<<"a">>, <<"bb">>], false},
        sse_all([<<"data:a\n\ndata:bb\n\n">>], thinwrap_llm_sse:new(8))
    ).

%% ---------------------------------------------------------------------------
%% Public surface: dispatch, misuse is badarg, never {error, _}

all_providers() ->
    thinwrap_llm_spec:compat_providers() ++ [anthropic, bedrock, gemini].

provider_cfg(bedrock) ->
    #{
        provider => bedrock,
        region => <<"us-east-1">>,
        access_key_id => <<"a">>,
        secret_access_key => <<"s">>
    };
provider_cfg(P) when P =:= 'azure-openai'; P =:= cloudflare ->
    #{provider => P, api_key => <<"k">>, base_url => <<"https://x/v1">>};
provider_cfg(P) ->
    #{provider => P, api_key => <<"k">>}.

supports_test() ->
    [?assert(thinwrap_llm:supports(P, complete)) || P <- all_providers()],
    [?assert(thinwrap_llm:supports(P, stream)) || P <- all_providers()],
    Emb = [P || P <- all_providers(), thinwrap_llm:supports(P, embed)],
    ?assertEqual(
        [
            openai,
            'azure-openai',
            openrouter,
            together,
            fireworks,
            mistral,
            deepinfra,
            cloudflare,
            vllm,
            ollama,
            lmstudio
        ],
        Emb
    ),
    ?assertNot(thinwrap_llm:supports(nope, complete)),
    ?assertNot(thinwrap_llm:supports(openai, fly)).

all_chat_providers_dispatch_test_() ->
    [
        ?_test(begin
            {C, P} = ?F:config(provider_cfg(Provider), [
                thinwrap_llm_fake_transport:resp(500, <<"{}">>)
            ]),
            ?assertMatch(
                {error, #{code := provider_unavailable, status := 500}},
                thinwrap_llm:complete(C, one_user())
            ),
            ?assertEqual(1, length(?F:calls(P)))
        end)
     || Provider <- all_providers()
    ].

embeddings_subset_dispatch_test_() ->
    [
        ?_test(begin
            {C, _} = ?F:config(provider_cfg(Provider), [
                thinwrap_llm_fake_transport:resp(500, <<"{}">>)
            ]),
            Call = fun() -> thinwrap_llm:embed(C, #{model => <<"m">>, input => [<<"a">>]}) end,
            case thinwrap_llm:supports(Provider, embed) of
                true -> ?assertMatch({error, #{code := provider_unavailable}}, Call());
                false -> ?assertError({badarg, _}, Call())
            end
        end)
     || Provider <- all_providers()
    ].

misuse_is_badarg_test_() ->
    O = #{provider => openai, api_key => <<"k">>},
    U = one_user(),
    [
        ?_assertError({badarg, _}, thinwrap_llm:complete(undefined, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(#{provider => 'not-a-provider'}, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(#{provider => <<"openai">>}, U)),
        %% base_url is required where there is no public default
        ?_assertError({badarg, _}, thinwrap_llm:complete(#{provider => 'azure-openai'}, U)),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(#{provider => cloudflare, base_url => <<>>}, U)
        ),
        %% a native provider's required credentials
        ?_assertError({badarg, _}, thinwrap_llm:complete(#{provider => anthropic}, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(#{provider => gemini}, U)),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(#{provider => bedrock, region => <<"us-east-1">>}, U)
        ),
        %% another provider's config shape (Python: the wrong config class)
        ?_assertError({badarg, _}, thinwrap_llm:complete(O#{anthropic_version => <<"x">>}, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O#{apikey => <<"x">>}, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O#{api_key => 42}, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O#{headers => #{<<"a">> => 1}}, U)),
        ?_assertError(
            {badarg, _},
            thinwrap_llm:complete(
                #{provider => anthropic, api_key => <<"k">>, default_max_tokens => -1}, U
            )
        ),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O#{transport => "x"}, U)),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(O#{transport => {no_such_mod_xyz, #{}}}, U)
        ),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O#{transport => {lists, #{}}}, U)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, not_a_map)),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, #{messages => []})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{model => "m"})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{temprature => 1})),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(O, U#{messages => [#{content => <<"x">>}]})
        ),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(O, U#{messages => [#{role => <<"user">>}]})
        ),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(O, U#{messages => [#{role => developer}]})
        ),
        ?_assertError(
            {badarg, _},
            thinwrap_llm:complete(O, U#{
                messages => [#{role => user, content_parts => [#{type => video}]}]
            })
        ),
        ?_assertError(
            {badarg, _},
            thinwrap_llm:complete(O, U#{
                messages => [#{role => assistant, tool_calls => [#{id => <<"a">>}]}]
            })
        ),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{tools => [#{}]})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{tool_choice => #{mode => any}})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{response_format => #{type => xml}})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{temperature => <<"0.5">>})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{max_output_tokens => 1.5})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{stop => <<"x">>})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{reasoning => #{effort => extreme}})),
        ?_assertError({badarg, _}, thinwrap_llm:complete(O, U#{passthrough => #{bdy => #{}}})),
        ?_assertError(
            {badarg, _}, thinwrap_llm:complete(O, U#{passthrough => #{query => #{a => [1]}}})
        ),
        ?_assertError({badarg, _}, thinwrap_llm:stream(O, U, not_a_fun, ok)),
        ?_assertError({badarg, _}, thinwrap_llm:embed(O, #{model => <<"m">>, input => <<"a">>})),
        ?_assertError(
            {badarg, _},
            thinwrap_llm:embed(O, #{model => <<"m">>, input => [<<"a">>], dimensions => <<"8">>})
        )
    ].

badarg_never_echoes_the_secret_test() ->
    try
        thinwrap_llm:complete(
            #{provider => openai, api_key => <<"supersecret">>, apikey => 1}, one_user()
        )
    of
        _ -> ?assert(false)
    catch
        error:Reason:Stack -> no_secret({Reason, Stack}, <<"supersecret">>)
    end.

secret_fun_is_resolved_at_use_test() ->
    Ok = <<"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}">>,
    {C, P} = ?F:config(#{provider => openai, api_key => fun() -> <<"from-fun">> end}, [
        thinwrap_llm_fake_transport:resp(200, Ok)
    ]),
    {ok, _} = thinwrap_llm:complete(C, one_user()),
    ?assertEqual(
        <<"Bearer from-fun">>, thinwrap_llm_fake_transport:header(?F:last(P), <<"Authorization">>)
    ),
    {C2, _} = ?F:config(#{provider => openai, api_key => fun() -> nope end}, [
        thinwrap_llm_fake_transport:resp(200, Ok)
    ]),
    ?assertError({badarg, _}, thinwrap_llm:complete(C2, one_user())).

error_codes_test() ->
    ?assertEqual(
        [
            rate_limited,
            auth_failed,
            provider_unavailable,
            invalid_request,
            context_length_exceeded,
            content_filtered,
            unknown
        ],
        thinwrap_llm_error:codes()
    ).

json_encode_invalid_utf8_is_invalid_request_test() ->
    {C, P} = cfg(openai, [thinwrap_llm_fake_transport:resp(200, <<"{}">>)]),
    ?assertMatch(
        {error, #{code := invalid_request, cause := invalid_byte}},
        thinwrap_llm:complete(C, #{
            model => <<"m">>, messages => [#{role => user, content => <<255>>}]
        })
    ),
    ?assertEqual([], ?F:calls(P)).
