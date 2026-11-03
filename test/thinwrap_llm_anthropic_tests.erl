-module(thinwrap_llm_anthropic_tests).

%% Ported from llm-py tests/test_anthropic.py: Messages-API request build,
%% complete() parse (text + tool_use), streaming over Anthropic SSE events
%% (delivered in 7-byte chunks by the fake transport), provider_metadata
%% replay, and error mapping.

-include_lib("eunit/include/eunit.hrl").

-define(F, thinwrap_llm_fake_transport).
-define(SSE, #{<<"content-type">> => <<"text/event-stream">>}).
-define(MODEL, <<"claude-opus-5-5">>).
-define(EMPTY_OK, <<"{\"content\":[],\"usage\":{},\"stop_reason\":\"end_turn\"}">>).

chat(Responses) -> chat(#{}, Responses).

chat(Extra, Responses) ->
    ?F:config(maps:merge(#{provider => anthropic, api_key => <<"k">>}, Extra), Responses).

user(Text) -> #{role => user, content => Text}.

input(Msgs) -> #{model => ?MODEL, messages => Msgs}.

x() -> input([user(<<"x">>)]).

collect(C, Input) ->
    case thinwrap_llm:stream(C, Input, fun(D, Acc) -> [D | Acc] end, []) of
        {ok, Ds} -> {ok, lists:reverse(Ds)};
        Err -> Err
    end.

sent(P) -> thinwrap_llm_fake_transport:body_json(?F:last(P)).

%% ---------------------------------------------------------------------------

complete_success_request_and_result_test() ->
    Body = json:encode(#{
        model => ?MODEL,
        stop_reason => end_turn,
        usage => #{input_tokens => 11, output_tokens => 7},
        content => [#{type => text, text => <<"Hello ">>}, #{type => text, text => <<"there">>}]
    }),
    {C, P} = chat(#{api_key => <<"sk-ant">>}, [thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(
        C,
        input([
            #{role => system, content => <<"Be brief.">>}, user(<<"Hi">>)
        ])
    ),
    ?assertMatch(
        #{
            message := #{content := <<"Hello there">>, tool_calls := []},
            finish_reason := stop,
            usage := #{input_tokens := 11, output_tokens := 7, total_tokens := 18},
            model := ?MODEL
        },
        R
    ),
    Last = ?F:last(P),
    ?assertEqual(<<"/v1/messages">>, thinwrap_llm_fake_transport:path_of(Last)),
    ?assertEqual(<<"https://api.anthropic.com/v1/messages">>, maps:get(url, Last)),
    ?assertEqual(<<"sk-ant">>, thinwrap_llm_fake_transport:header(Last, <<"x-api-key">>)),
    ?assertEqual(
        <<"2023-06-01">>, thinwrap_llm_fake_transport:header(Last, <<"anthropic-version">>)
    ),
    ?assertEqual(
        <<"application/json">>, thinwrap_llm_fake_transport:header(Last, <<"Content-Type">>)
    ),
    S = sent(P),
    ?assertEqual(<<"Be brief.">>, maps:get(<<"system">>, S)),
    ?assertEqual(16000, maps:get(<<"max_tokens">>, S)),
    ?assertEqual(
        [
            #{
                <<"role">> => <<"user">>,
                <<"content">> => [#{<<"type">> => <<"text">>, <<"text">> => <<"Hi">>}]
            }
        ],
        maps:get(<<"messages">>, S)
    ),
    ?assertNot(maps:is_key(<<"response_format">>, S)),
    ?assertNot(maps:is_key(<<"stream">>, S)).

complete_tool_call_result_test() ->
    Body = json:encode(#{
        model => ?MODEL,
        stop_reason => tool_use,
        usage => #{input_tokens => 20, output_tokens => 15},
        content => [
            #{type => text, text => <<"Let me check.">>},
            #{
                type => tool_use,
                id => <<"toolu_1">>,
                name => get_weather,
                input => #{city => <<"SF">>}
            }
        ]
    }),
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, (input([user(<<"weather?">>)]))#{
        tools => [
            #{
                name => <<"get_weather">>,
                description => <<"Get weather">>,
                parameters => #{type => object}
            }
        ],
        tool_choice => #{mode => required}
    }),
    ?assertMatch(#{finish_reason := tool_calls}, R),
    #{message := #{tool_calls := [TC]}} = R,
    ?assertMatch(#{id := <<"toolu_1">>, name := <<"get_weather">>}, TC),
    ?assertEqual(#{<<"city">> => <<"SF">>}, json:decode(maps:get(arguments, TC))),
    S = sent(P),
    ?assertEqual(
        [
            #{
                <<"name">> => <<"get_weather">>,
                <<"description">> => <<"Get weather">>,
                <<"input_schema">> => #{<<"type">> => <<"object">>}
            }
        ],
        maps:get(<<"tools">>, S)
    ),
    ?assertEqual(#{<<"type">> => <<"any">>}, maps:get(<<"tool_choice">>, S)).

tool_without_parameters_and_tool_choice_modes_test_() ->
    [
        ?_test(begin
            {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
            {ok, _} = thinwrap_llm:complete(C, (x())#{
                tools => [#{name => <<"f">>}], tool_choice => TC
            }),
            S = sent(P),
            ?assertEqual(
                [
                    #{
                        <<"name">> => <<"f">>,
                        <<"input_schema">> => #{<<"type">> => <<"object">>, <<"properties">> => #{}}
                    }
                ],
                maps:get(<<"tools">>, S)
            ),
            ?assertEqual(Want, maps:get(<<"tool_choice">>, S))
        end)
     || {TC, Want} <- [
            {#{mode => auto}, #{<<"type">> => <<"auto">>}},
            {#{mode => none}, #{<<"type">> => <<"none">>}},
            {#{mode => function, function_name => <<"f">>}, #{
                <<"type">> => <<"tool">>, <<"name">> => <<"f">>
            }}
        ]
    ].

tool_result_folds_into_user_turn_test() ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => <<"m">>,
        messages => [
            #{
                role => assistant,
                tool_calls => [
                    #{
                        id => <<"toolu_1">>,
                        name => <<"get_weather">>,
                        arguments => <<"{\"city\":\"SF\"}">>
                    }
                ]
            },
            #{role => tool, tool_call_id => <<"toolu_1">>, content => <<"sunny">>},
            #{role => tool, tool_call_id => <<"toolu_2">>, content => <<"warm">>}
        ]
    }),
    ?assertEqual(
        [
            #{
                <<"role">> => <<"assistant">>,
                <<"content">> => [
                    #{
                        <<"type">> => <<"tool_use">>,
                        <<"id">> => <<"toolu_1">>,
                        <<"name">> => <<"get_weather">>,
                        <<"input">> => #{<<"city">> => <<"SF">>}
                    }
                ]
            },
            #{
                <<"role">> => <<"user">>,
                <<"content">> => [
                    #{
                        <<"type">> => <<"tool_result">>,
                        <<"tool_use_id">> => <<"toolu_1">>,
                        <<"content">> => <<"sunny">>
                    },
                    #{
                        <<"type">> => <<"tool_result">>,
                        <<"tool_use_id">> => <<"toolu_2">>,
                        <<"content">> => <<"warm">>
                    }
                ]
            }
        ],
        maps:get(<<"messages">>, sent(P))
    ).

multiple_system_messages_are_joined_test() ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(
        C,
        input([
            #{role => system, content => <<"A">>},
            #{role => system, content_parts => [#{type => text, text => <<"B">>}]},
            user(<<"hi">>)
        ])
    ),
    ?assertEqual(<<"A\n\nB">>, maps:get(<<"system">>, sent(P))).

reasoning_effort_sends_adaptive_thinking_and_effort_test() ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, (input([user(<<"think">>)]))#{
        reasoning => #{effort => high}, temperature => 0.7, top_p => 0.9
    }),
    S = sent(P),
    ?assertEqual(#{<<"type">> => <<"adaptive">>}, maps:get(<<"thinking">>, S)),
    ?assertEqual(#{<<"effort">> => <<"high">>}, maps:get(<<"output_config">>, S)),
    %% Adaptive thinking has no budget, so the default max_tokens is sent as-is.
    ?assertEqual(16000, maps:get(<<"max_tokens">>, S)),
    ?assertEqual(nomatch, binary:match(maps:get(body, ?F:last(P)), <<"budget_tokens">>)),
    %% sampling omitted while thinking on
    ?assertNot(maps:is_key(<<"temperature">>, S)),
    ?assertNot(maps:is_key(<<"top_p">>, S)).

sampling_sent_without_reasoning_test() ->
    {C, P} = chat(#{default_max_tokens => 500}, [thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, (x())#{temperature => 0.7, top_p => 0.9}),
    S = sent(P),
    ?assertEqual(0.7, maps:get(<<"temperature">>, S)),
    ?assertEqual(0.9, maps:get(<<"top_p">>, S)),
    ?assertEqual(500, maps:get(<<"max_tokens">>, S)),
    ?assertNot(maps:is_key(<<"thinking">>, S)).

reasoning_with_small_explicit_max_tokens_is_sent_unchanged_test() ->
    {C, P} = chat(#{default_max_tokens => 500}, [thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, (input([user(<<"think">>)]))#{
        reasoning => #{effort => low}, max_output_tokens => 100
    }),
    S = sent(P),
    ?assertEqual(100, maps:get(<<"max_tokens">>, S)),
    ?assertEqual(#{<<"effort">> => <<"low">>}, maps:get(<<"output_config">>, S)).

budget_thinking_via_passthrough_without_reasoning_test() ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => <<"claude-haiku-4-5">>,
        messages => [user(<<"think">>)],
        max_output_tokens => 8000,
        passthrough => #{body => #{thinking => #{type => enabled, budget_tokens => 2048}}}
    }),
    S = sent(P),
    ?assertEqual(
        #{<<"type">> => <<"enabled">>, <<"budget_tokens">> => 2048}, maps:get(<<"thinking">>, S)
    ),
    ?assertNot(maps:is_key(<<"output_config">>, S)).

image_part_and_stop_sequences_and_passthrough_test() ->
    {C, P} = chat(
        #{anthropic_version => <<"2099-01-01">>, headers => #{<<"x-extra">> => <<"1">>}},
        [thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]
    ),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => <<"m">>,
        messages => [
            #{
                role => user,
                content_parts => [
                    #{type => image, base64 => <<"BASE64DATA">>, media_type => <<"image/png">>}
                ]
            }
        ],
        stop => [<<"STOP">>, <<"END">>],
        passthrough => #{
            body => #{metadata => #{user_id => <<"u1">>}},
            headers => #{<<"x-req">> => <<"yes">>},
            query => #{<<"beta">> => <<"true">>}
        }
    }),
    S = sent(P),
    ?assertEqual(
        [
            #{
                <<"type">> => <<"image">>,
                <<"source">> => #{
                    <<"type">> => <<"base64">>,
                    <<"media_type">> => <<"image/png">>,
                    <<"data">> => <<"BASE64DATA">>
                }
            }
        ],
        maps:get(<<"content">>, hd(maps:get(<<"messages">>, S)))
    ),
    ?assertEqual([<<"STOP">>, <<"END">>], maps:get(<<"stop_sequences">>, S)),
    ?assertEqual(#{<<"user_id">> => <<"u1">>}, maps:get(<<"metadata">>, S)),
    Last = ?F:last(P),
    ?assertEqual(
        <<"2099-01-01">>, thinwrap_llm_fake_transport:header(Last, <<"anthropic-version">>)
    ),
    ?assertEqual(<<"1">>, thinwrap_llm_fake_transport:header(Last, <<"x-extra">>)),
    ?assertEqual(<<"yes">>, thinwrap_llm_fake_transport:header(Last, <<"x-req">>)),
    ?assertEqual(<<"https://api.anthropic.com/v1/messages?beta=true">>, maps:get(url, Last)).

base_url_override_is_trimmed_test() ->
    {C, P} = chat(#{base_url => <<"https://proxy.example/anthropic/v1/">>}, [
        thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)
    ]),
    {ok, _} = thinwrap_llm:complete(C, x()),
    ?assertEqual(<<"https://proxy.example/anthropic/v1/messages">>, maps:get(url, ?F:last(P))).

stream_text_and_tool_and_usage_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":25}}}">>,
        <<"{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}">>,
        <<"{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}">>,
        <<"{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}">>,
        <<"{\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_9\",\"name\":\"f\"}}">>,
        <<"{\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":1}\"}}">>,
        <<"{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":8}}">>,
        <<"{\"type\":\"message_stop\"}">>
    ]),
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, input([user(<<"hi">>)])),
    ?assertEqual(<<"Hello">>, iolist_to_binary([maps:get(content_delta, D, <<>>) || D <- Ds])),
    %% Anthropic's content-block index is 1 (a text block preceded it), but the
    %% normalized tool-call index is the 0-based tool-call ordinal.
    ?assertEqual(
        [#{index => 0, id => <<"toolu_9">>, function_name => <<"f">>}],
        [T || #{tool_call_delta := #{function_name := _} = T} <- Ds]
    ),
    ?assertEqual(
        <<"{\"a\":1}">>,
        iolist_to_binary([A || #{tool_call_delta := #{arguments_delta := A}} <- Ds])
    ),
    ?assertEqual([stop], [F || #{finish_reason := F} <- Ds]),
    ?assertEqual(
        [#{input_tokens => 25, output_tokens => 8, total_tokens => 33}], [U || #{usage := U} <- Ds]
    ),
    ?assertEqual(true, maps:get(<<"stream">>, sent(P))),
    ?assertEqual(
        <<"text/event-stream">>, thinwrap_llm_fake_transport:header(?F:last(P), <<"Accept">>)
    ).

error_mapping_rate_limited_test() ->
    {C, _} = chat([
        thinwrap_llm_fake_transport:resp(
            429,
            <<"{\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}">>,
            #{<<"Retry-After">> => <<"30">>}
        )
    ]),
    {error, E} = thinwrap_llm:complete(C, x()),
    ?assertMatch(
        #{
            code := rate_limited,
            provider_message := <<"slow down">>,
            cause := #{<<"retryAfterSeconds">> := 30}
        },
        E
    ).

error_mapping_auth_and_unavailable_and_context_test_() ->
    [
        ?_test(begin
            {C, _} = chat([thinwrap_llm_fake_transport:resp(S, B)]),
            ?assertMatch({error, #{code := Want, status := S}}, thinwrap_llm:complete(C, x()))
        end)
     || {S, B, Want} <- [
            {401, <<"{\"error\":{\"message\":\"bad key\"}}">>, auth_failed},
            %% 529 → provider unavailable (Anthropic overloaded)
            {529, <<"{\"error\":{\"message\":\"overloaded\"}}">>, provider_unavailable},
            {400,
                <<"{\"error\":{\"message\":\"prompt is too long: 250000 tokens > max_tokens\"}}">>,
                context_length_exceeded},
            {400, <<"{\"error\":{\"message\":\"unknown field foo\"}}">>, invalid_request}
        ]
    ].

non_json_error_message_falls_back_to_http_status_test() ->
    {C, _} = chat([thinwrap_llm_fake_transport:resp(502, <<"<html>">>)]),
    ?assertMatch(
        {error, #{code := provider_unavailable, message := <<"HTTP 502">>}},
        thinwrap_llm:complete(C, x())
    ).

stream_error_frame_raises_test() ->
    Body = <<
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n"
        "event: error\n"
        "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"overloaded mid-stream\"}}\n\n"
    >>,
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    Self = self(),
    Ref = make_ref(),
    Result = thinwrap_llm:stream(
        C,
        input([user(<<"hi">>)]),
        fun(D, A) ->
            Self ! {Ref, D},
            A
        end,
        ok
    ),
    receive
        {Ref, D} -> ?assertEqual(<<"Hi">>, maps:get(content_delta, D))
    after 0 -> ?assert(false)
    end,
    ?assertMatch(
        {error, #{
            code := provider_unavailable,
            status := 200,
            provider_message := <<"overloaded mid-stream">>
        }},
        Result
    ).

requires_anthropic_config_test() ->
    %% Another provider's config shape (Python: the wrong config class) is misuse.
    ?assertError(
        {badarg, _},
        thinwrap_llm:complete(
            #{provider => anthropic, api_key => <<"k">>, region => <<"us-east-1">>}, x()
        )
    ),
    ?assertError({badarg, _}, thinwrap_llm:complete(#{provider => anthropic}, x())).

complete_200_without_content_array_is_empty_body_error_test_() ->
    [
        ?_test(begin
            {C, _} = chat([thinwrap_llm_fake_transport:resp(200, B)]),
            ?assertMatch(
                {error, #{code := provider_unavailable, status := 200}},
                thinwrap_llm:complete(C, x())
            )
        end)
     || B <- [
            <<"{}">>,
            <<"{\"id\":\"msg_1\",\"type\":\"message\",\"stop_reason\":\"end_turn\"}">>,
            <<"{\"content\":null}">>,
            <<>>
        ]
    ].

%% ---------------------------------------------------------------------------
%% provider_metadata: thinking blocks round-trip

%% A tool-use response with adaptive thinking, in the shape the Messages API
%% documents: thinking (with signature) and redacted_thinking blocks precede
%% the text and tool_use blocks.
thinking_content() ->
    [
        #{
            <<"type">> => <<"thinking">>,
            <<"thinking">> => <<"Need the weather tool.">>,
            <<"signature">> => <<"sig-abc">>
        },
        #{<<"type">> => <<"redacted_thinking">>, <<"data">> => <<"ENCRYPTED">>},
        #{<<"type">> => <<"text">>, <<"text">> => <<"Checking.">>},
        #{
            <<"type">> => <<"tool_use">>,
            <<"id">> => <<"toolu_01">>,
            <<"name">> => <<"get_weather">>,
            <<"input">> => #{<<"city">> => <<"Paris">>}
        }
    ].

tool_turn_result() ->
    Body = json:encode(#{
        model => ?MODEL,
        stop_reason => tool_use,
        usage => #{input_tokens => 10, output_tokens => 5},
        content => thinking_content()
    }),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, input([user(<<"weather?">>)])),
    R.

replay(Assistant) ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(
        C,
        input([
            user(<<"weather?">>),
            Assistant,
            #{role => tool, tool_call_id => <<"toolu_01">>, content => <<"sunny">>}
        ])
    ),
    {sent(P), maps:get(body, ?F:last(P))}.

assistant_from(#{message := #{content := Content, tool_calls := TCs, provider_metadata := PM}}) ->
    #{role => assistant, content => Content, tool_calls => TCs, provider_metadata => PM}.

complete_captures_content_blocks_verbatim_test() ->
    #{message := #{provider_metadata := PM}, raw := Raw} = tool_turn_result(),
    ?assertEqual(#{<<"anthropic">> => #{<<"content">> => thinking_content()}}, PM),
    ?assertEqual(4, length(maps:get(<<"content">>, Raw))).

replay_sends_content_verbatim_when_message_matches_test() ->
    {S, RawBody} = replay(assistant_from(tool_turn_result())),
    ?assertEqual(
        #{<<"role">> => <<"assistant">>, <<"content">> => thinking_content()},
        lists:nth(2, maps:get(<<"messages">>, S))
    ),
    ?assertEqual(nomatch, binary:match(RawBody, <<"provider_metadata">>)).

replay_rebuilds_when_text_was_edited_test() ->
    A = assistant_from(tool_turn_result()),
    {S, _} = replay(A#{content := <<"Checking now.">>}),
    ?assertEqual(
        [
            #{<<"type">> => <<"text">>, <<"text">> => <<"Checking now.">>},
            #{
                <<"type">> => <<"tool_use">>,
                <<"id">> => <<"toolu_01">>,
                <<"name">> => <<"get_weather">>,
                <<"input">> => #{<<"city">> => <<"Paris">>}
            }
        ],
        maps:get(<<"content">>, lists:nth(2, maps:get(<<"messages">>, S)))
    ).

replay_rebuilds_when_tool_ids_differ_test() ->
    A = assistant_from(tool_turn_result()),
    {S, _} = replay(A#{
        tool_calls := [
            #{
                id => <<"toolu_99">>,
                name => <<"get_weather">>,
                arguments => <<"{\"city\":\"Paris\"}">>
            }
        ]
    }),
    Blocks = maps:get(<<"content">>, lists:nth(2, maps:get(<<"messages">>, S))),
    ?assertEqual([<<"text">>, <<"tool_use">>], [maps:get(<<"type">>, B) || B <- Blocks]),
    ?assertEqual(<<"toolu_99">>, maps:get(<<"id">>, lists:nth(2, Blocks))).

replay_ignores_foreign_provider_metadata_test() ->
    {S, RawBody} = replay(#{
        role => assistant,
        content => <<"Checking.">>,
        tool_calls => [
            #{
                id => <<"toolu_01">>,
                name => <<"get_weather">>,
                arguments => <<"{\"city\":\"Paris\"}">>,
                provider_metadata => #{<<"gemini">> => #{<<"thoughtSignature">> => <<"g-sig">>}}
            }
        ],
        provider_metadata => #{
            <<"gemini">> => #{
                <<"content">> => [#{<<"type">> => <<"text">>, <<"text">> => <<"Checking.">>}]
            }
        }
    }),
    ?assertEqual(
        [
            #{<<"type">> => <<"text">>, <<"text">> => <<"Checking.">>},
            #{
                <<"type">> => <<"tool_use">>,
                <<"id">> => <<"toolu_01">>,
                <<"name">> => <<"get_weather">>,
                <<"input">> => #{<<"city">> => <<"Paris">>}
            }
        ],
        maps:get(<<"content">>, lists:nth(2, maps:get(<<"messages">>, S)))
    ),
    ?assertEqual(nomatch, binary:match(RawBody, <<"g-sig">>)).

stream_rebuilds_the_same_content_array_test() ->
    Body = thinwrap_llm_fake_transport:sse_events([
        #{type => message_start, message => #{usage => #{input_tokens => 10}}},
        #{
            type => content_block_start,
            index => 0,
            content_block => #{type => thinking, thinking => <<>>, signature => <<>>}
        },
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => thinking_delta, thinking => <<"Need the ">>}
        },
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => thinking_delta, thinking => <<"weather tool.">>}
        },
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => signature_delta, signature => <<"sig-abc">>}
        },
        #{type => content_block_stop, index => 0},
        #{
            type => content_block_start,
            index => 1,
            content_block => #{type => redacted_thinking, data => <<"ENCRYPTED">>}
        },
        #{type => content_block_stop, index => 1},
        #{type => content_block_start, index => 2, content_block => #{type => text, text => <<>>}},
        #{
            type => content_block_delta,
            index => 2,
            delta => #{type => text_delta, text => <<"Checking.">>}
        },
        #{type => content_block_stop, index => 2},
        #{
            type => content_block_start,
            index => 3,
            content_block => #{
                type => tool_use, id => <<"toolu_01">>, name => get_weather, input => #{}
            }
        },
        #{
            type => content_block_delta,
            index => 3,
            delta => #{type => input_json_delta, partial_json => <<"{\"city\":">>}
        },
        #{
            type => content_block_delta,
            index => 3,
            delta => #{type => input_json_delta, partial_json => <<"\"Paris\"}">>}
        },
        #{type => content_block_stop, index => 3},
        #{
            type => message_delta,
            delta => #{stop_reason => tool_use},
            usage => #{output_tokens => 5}
        },
        #{type => message_stop}
    ]),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, input([user(<<"weather?">>)])),
    Terminal = lists:last(Ds),
    ?assertEqual(tool_calls, maps:get(finish_reason, Terminal)),
    ?assertEqual(
        #{<<"anthropic">> => #{<<"content">> => thinking_content()}},
        maps:get(provider_metadata, Terminal)
    ),
    %% Only the terminal delta carries it.
    ?assert(lists:all(fun(D) -> not maps:is_key(provider_metadata, D) end, lists:droplast(Ds))),
    #{message := #{provider_metadata := PM}} = tool_turn_result(),
    ?assertEqual(PM, maps:get(provider_metadata, Terminal)).

stream_server_tool_input_is_not_a_caller_tool_call_test() ->
    Body = thinwrap_llm_fake_transport:sse_events([
        #{type => message_start, message => #{usage => #{input_tokens => 1}}},
        #{
            type => content_block_start,
            index => 0,
            content_block => #{
                type => server_tool_use, id => <<"srvtoolu_1">>, name => web_search, input => #{}
            }
        },
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => input_json_delta, partial_json => <<"{\"query\":\"x\"}">>}
        },
        #{type => content_block_stop, index => 0},
        #{
            type => content_block_start,
            index => 1,
            content_block => #{type => tool_use, id => <<"toolu_1">>, name => f, input => #{}}
        },
        #{
            type => content_block_delta,
            index => 1,
            delta => #{type => input_json_delta, partial_json => <<"{}">>}
        },
        #{
            type => message_delta,
            delta => #{stop_reason => tool_use},
            usage => #{output_tokens => 2}
        }
    ]),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, x()),
    ?assertEqual(
        [
            #{index => 0, id => <<"toolu_1">>, function_name => <<"f">>},
            #{index => 0, arguments_delta => <<"{}">>}
        ],
        [T || #{tool_call_delta := T} <- Ds]
    ),
    #{<<"anthropic">> := #{<<"content">> := [First | _]}} = maps:get(
        provider_metadata, lists:last(Ds)
    ),
    ?assertEqual(#{<<"query">> => <<"x">>}, maps:get(<<"input">>, First)).

%% ---------------------------------------------------------------------------
%% usage, stop reasons, error classification

usage_counts_cache_read_and_write_as_input_test() ->
    Body = json:encode(#{
        content => [#{type => text, text => <<"ok">>}],
        stop_reason => end_turn,
        usage => #{
            input_tokens => 5,
            cache_read_input_tokens => 100,
            cache_creation_input_tokens => 20,
            output_tokens => 7
        }
    }),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body)]),
    ?assertMatch(
        {ok, #{usage := #{input_tokens := 125, output_tokens := 7, total_tokens := 132}}},
        thinwrap_llm:complete(C, x())
    ).

last_usage(Events) ->
    {C, _} = chat([
        thinwrap_llm_fake_transport:resp(200, thinwrap_llm_fake_transport:sse_events(Events), ?SSE)
    ]),
    {ok, Ds} = collect(C, x()),
    lists:last([U || #{usage := U} <- Ds]).

stream_usage_counts_cache_tokens_test() ->
    ?assertEqual(
        #{input_tokens => 125, output_tokens => 7, total_tokens => 132},
        last_usage([
            #{
                type => message_start,
                message => #{
                    usage => #{
                        input_tokens => 5,
                        cache_read_input_tokens => 100,
                        cache_creation_input_tokens => 20,
                        output_tokens => 1
                    }
                }
            },
            #{
                type => message_delta,
                delta => #{stop_reason => end_turn},
                usage => #{output_tokens => 7}
            }
        ])
    ).

stop_reason_mapping_test_() ->
    [
        ?_test(begin
            Body = json:encode(#{
                content => [#{type => text, text => <<"x">>}], stop_reason => SR, usage => #{}
            }),
            {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body)]),
            ?assertMatch({ok, #{finish_reason := Want}}, thinwrap_llm:complete(C, x()))
        end)
     || {SR, Want} <- [
            {pause_turn, unknown},
            {refusal, content_filter},
            {model_context_window_exceeded, length},
            {max_tokens, length},
            {stop_sequence, stop},
            {end_turn, stop}
        ]
    ].

error_classification_test_() ->
    [
        ?_test(begin
            Body = json:encode(#{
                type => error, error => #{type => invalid_request_error, message => Msg}
            }),
            {C, _} = chat([thinwrap_llm_fake_transport:resp(S, Body)]),
            ?assertMatch({error, #{code := Want}}, thinwrap_llm:complete(C, x()))
        end)
     || {S, Msg, Want} <- [
            {400, <<"prompt is too long: 208310 tokens > 200000 maximum">>,
                context_length_exceeded},
            {400,
                <<"input length and `max_tokens` exceed context limit: 188240 + 21333 > 200000, decrease input length or `max_tokens` and try again">>,
                context_length_exceeded},
            {400, <<"max_tokens: Field required">>, invalid_request},
            {400, <<"context_management: Extra inputs are not permitted">>, invalid_request},
            {400, <<"tool_choice: type \"tool\" and \"any\" are not supported for this model.">>,
                invalid_request},
            {402, <<"There's an issue with your billing or payment information.">>, rate_limited},
            {413, <<"Request exceeds the maximum allowed number of bytes.">>, invalid_request},
            {504, <<"Request timed out.">>, provider_unavailable},
            %% review-5
            {408, <<"Request timeout">>, provider_unavailable},
            {409, <<"conflict">>, invalid_request},
            {400, <<"context window is too small for this tool">>, invalid_request}
        ]
    ].

stream_error_frame_classified_by_type_test_() ->
    [
        ?_test(begin
            {C, _} = chat([
                thinwrap_llm_fake_transport:resp(
                    200,
                    thinwrap_llm_fake_transport:sse_events([
                        #{type => error, error => #{type => T, message => Msg}}
                    ]),
                    ?SSE
                )
            ]),
            ?assertMatch({error, #{code := Want, status := 200}}, collect(C, x()))
        end)
     || {T, Msg, Want} <- [
            {invalid_request_error, <<"prompt is too long: 208310 tokens > 200000 maximum">>,
                context_length_exceeded},
            {invalid_request_error, <<"max_tokens: Field required">>, invalid_request},
            {rate_limit_error, <<"slow">>, rate_limited},
            {billing_error, <<"billing">>, rate_limited},
            {timeout_error, <<"timeout">>, provider_unavailable},
            {authentication_error, <<"no">>, auth_failed},
            {not_found_error, <<"no">>, invalid_request},
            {brand_new_error, <<"?">>, unknown}
        ]
    ].

%% ---------------------------------------------------------------------------
%% review-5

replay_history(Assistants) ->
    Msgs =
        [user(<<"go">>)] ++
            lists:append([
                [
                    A
                    | [
                        #{role => tool, tool_call_id => Id, content => <<"ok">>}
                     || #{id := Id} <- maps:get(tool_calls, A)
                    ]
                ]
             || A <- Assistants
            ]),
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, input(Msgs)),
    [
        maps:get(<<"content">>, M)
     || #{<<"role">> := <<"assistant">>} = M <- maps:get(<<"messages">>, sent(P))
    ].

turn(N) -> turn(N, #{}).

turn(N, Opts) ->
    Nb = integer_to_binary(N),
    Id = <<"toolu_", Nb/binary>>,
    Content = [
        #{
            <<"type">> => <<"thinking">>,
            <<"thinking">> => <<"t", Nb/binary>>,
            <<"signature">> => <<"s", Nb/binary>>
        },
        #{<<"type">> => <<"text">>, <<"text">> => <<"step ", Nb/binary>>},
        #{
            <<"type">> => <<"tool_use">>,
            <<"id">> => Id,
            <<"name">> => <<"f">>,
            <<"input">> => #{<<"n">> => N}
        }
    ],
    #{
        role => assistant,
        content => maps:get(text, Opts, <<"step ", Nb/binary>>),
        tool_calls => [
            #{
                id => Id,
                name => <<"f">>,
                arguments => maps:get(args, Opts, iolist_to_binary(json:encode(#{n => N})))
            }
        ],
        provider_metadata => #{<<"anthropic">> => #{<<"content">> => Content}}
    }.

types(Blocks) -> [maps:get(<<"type">>, B) || B <- Blocks].

every_turn_after_a_rebuilt_one_is_rebuilt_test() ->
    [S1, S2, S3] = replay_history([turn(1), turn(2, #{text => <<"edited">>}), turn(3)]),
    %% before the edit: verbatim
    ?assertEqual(<<"thinking">>, maps:get(<<"type">>, hd(S1))),
    %% edited: rebuilt
    ?assertEqual([<<"text">>, <<"tool_use">>], types(S2)),
    %% after it: rebuilt too
    ?assertEqual([<<"text">>, <<"tool_use">>], types(S3)).

all_turns_verbatim_when_none_is_edited_test() ->
    Sent = replay_history([turn(1), turn(2), turn(3)]),
    ?assert(lists:all(fun(T) -> maps:get(<<"type">>, hd(T)) =:= <<"thinking">> end, Sent)).

edited_tool_arguments_force_a_rebuild_test() ->
    [S] = replay_history([turn(1, #{args => <<"{\"n\":99}">>})]),
    ?assertEqual([<<"text">>, <<"tool_use">>], types(S)),
    ?assertEqual(#{<<"n">> => 99}, maps:get(<<"input">>, lists:nth(2, S))).

reformatted_but_equal_arguments_still_replay_verbatim_test() ->
    [S] = replay_history([turn(1, #{args => <<"{ \"n\" : 1 }">>})]),
    ?assertEqual(<<"thinking">>, maps:get(<<"type">>, hd(S))).

malformed_arguments_force_a_rebuild_test() ->
    [S] = replay_history([turn(1, #{args => <<"{not json">>})]),
    ?assertEqual([<<"text">>, <<"tool_use">>], types(S)),
    %% an unparseable arguments text is sent as an empty input object
    ?assertEqual(#{}, maps:get(<<"input">>, lists:nth(2, S))).

%% A nested empty JSON object must survive as an object (never an array) in
%% the tool arguments and on replay.
empty_object_input_survives_test() ->
    Body = json:encode(#{
        stop_reason => tool_use,
        usage => #{},
        content => [#{type => tool_use, id => <<"toolu_1">>, name => f, input => #{opts => #{}}}]
    }),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, x()),
    #{message := #{tool_calls := [TC]}} = R,
    ?assertEqual(<<"{\"opts\":{}}">>, maps:get(arguments, TC)),
    {S, RawBody} = replay((assistant_from(R))#{tool_calls := [TC#{id := <<"toolu_01">>}]}),
    %% ids differ → rebuilt from the arguments text
    ?assertEqual(
        #{<<"opts">> => #{}},
        maps:get(
            <<"input">>, hd(maps:get(<<"content">>, lists:nth(2, maps:get(<<"messages">>, S))))
        )
    ),
    ?assertNotEqual(nomatch, binary:match(RawBody, <<"{\"opts\":{}}">>)),
    {S2, _} = replay(assistant_from(R)),
    ?assertEqual(
        [
            #{
                <<"type">> => <<"tool_use">>,
                <<"id">> => <<"toolu_1">>,
                <<"name">> => <<"f">>,
                <<"input">> => #{<<"opts">> => #{}}
            }
        ],
        maps:get(<<"content">>, lists:nth(2, maps:get(<<"messages">>, S2)))
    ).

stream_usage_merges_field_by_field_test() ->
    ?assertEqual(
        #{input_tokens => 150, output_tokens => 5, total_tokens => 155},
        last_usage([
            #{
                type => message_start,
                message => #{
                    usage => #{
                        input_tokens => 100,
                        cache_read_input_tokens => 30,
                        cache_creation_input_tokens => 20,
                        output_tokens => 1
                    }
                }
            },
            %% null and absent fields keep message_start's values.
            #{
                type => message_delta,
                delta => #{stop_reason => end_turn},
                usage => #{input_tokens => null, output_tokens => 5}
            }
        ])
    ).

stream_usage_partial_restatement_keeps_cache_counts_test() ->
    ?assertMatch(
        #{input_tokens := 160, total_tokens := 165},
        last_usage([
            #{
                type => message_start,
                message => #{usage => #{input_tokens => 100, cache_read_input_tokens => 50}}
            },
            #{
                type => message_delta,
                delta => #{stop_reason => end_turn},
                usage => #{input_tokens => 110, output_tokens => 5}
            }
        ])
    ).

stream_keeps_start_input_when_no_deltas_follow_test() ->
    Body = thinwrap_llm_fake_transport:sse_events([
        #{
            type => content_block_start,
            index => 0,
            content_block => #{
                type => server_tool_use,
                id => <<"srv_1">>,
                name => web_search,
                input => #{query => <<"q">>}
            }
        },
        #{type => content_block_stop, index => 0},
        #{
            type => message_delta,
            delta => #{stop_reason => end_turn},
            usage => #{output_tokens => 1}
        }
    ]),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, x()),
    #{<<"anthropic">> := #{<<"content">> := [B]}} = maps:get(provider_metadata, lists:last(Ds)),
    ?assertEqual(#{<<"query">> => <<"q">>}, maps:get(<<"input">>, B)).

stream_provider_metadata_only_on_the_stop_delta_test() ->
    Body = thinwrap_llm_fake_transport:sse_events([
        #{type => content_block_start, index => 0, content_block => #{type => text, text => <<>>}},
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => text_delta, text => <<"Hi">>}
        },
        #{type => message_delta, delta => #{}, usage => #{output_tokens => 1}},
        #{
            type => message_delta,
            delta => #{stop_reason => end_turn},
            usage => #{output_tokens => 2}
        }
    ]),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, x()),
    WithMeta = [D || #{provider_metadata := _} = D <- Ds],
    ?assertMatch([#{finish_reason := stop}], WithMeta).

stream_citations_are_rebuilt_test() ->
    Cit = #{type => char_location, cited_text => <<"c">>},
    Body = thinwrap_llm_fake_transport:sse_events([
        #{type => content_block_start, index => 0, content_block => #{type => text, text => <<>>}},
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => citations_delta, citation => Cit}
        },
        #{
            type => content_block_delta,
            index => 0,
            delta => #{type => text_delta, text => <<"Hi">>}
        },
        #{
            type => message_delta,
            delta => #{stop_reason => end_turn},
            usage => #{output_tokens => 1}
        }
    ]),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Ds} = collect(C, x()),
    ?assertEqual(
        #{
            <<"anthropic">> => #{
                <<"content">> => [
                    #{
                        <<"type">> => <<"text">>,
                        <<"text">> => <<"Hi">>,
                        <<"citations">> => [
                            #{<<"type">> => <<"char_location">>, <<"cited_text">> => <<"c">>}
                        ]
                    }
                ]
            }
        },
        maps:get(provider_metadata, lists:last(Ds))
    ).

empty_text_parts_are_dropped_test() ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(
        C,
        input([
            #{
                role => user,
                content_parts => [
                    #{type => text, text => <<>>},
                    #{type => text, text => <<"hi">>},
                    #{type => text, text => <<>>}
                ]
            }
        ])
    ),
    ?assertEqual(
        [#{<<"type">> => <<"text">>, <<"text">> => <<"hi">>}],
        maps:get(<<"content">>, hd(maps:get(<<"messages">>, sent(P))))
    ).

empty_message_sends_one_empty_text_block_test() ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
    {ok, _} = thinwrap_llm:complete(C, input([#{role => user}])),
    ?assertEqual(
        [#{<<"type">> => <<"text">>, <<"text">> => <<>>}],
        maps:get(<<"content">>, hd(maps:get(<<"messages">>, sent(P))))
    ).

empty_effort_defaults_to_medium_and_unknown_is_sent_as_given_test_() ->
    [
        ?_test(begin
            {C, P} = chat([thinwrap_llm_fake_transport:resp(200, ?EMPTY_OK)]),
            {ok, _} = thinwrap_llm:complete(C, (x())#{reasoning => #{effort => Effort}}),
            ?assertEqual(#{<<"effort">> => Want}, maps:get(<<"output_config">>, sent(P)))
        end)
     || {Effort, Want} <- [{<<>>, <<"medium">>}, {<<"max">>, <<"max">>}]
    ].
