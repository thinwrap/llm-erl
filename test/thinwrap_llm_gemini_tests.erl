-module(thinwrap_llm_gemini_tests).

%% Ported from llm-py tests/test_gemini.py: complete() (request build +
%% parse), functionCall → tool call, streaming SSE, thought signatures, error
%% mapping.

-include_lib("eunit/include/eunit.hrl").

-define(F, thinwrap_llm_fake_transport).
-define(SSE, #{<<"content-type">> => <<"text/event-stream">>}).
-define(MODEL, <<"gemini-3.8-flash">>).

chat(Responses) -> ?F:config(#{provider => gemini, api_key => <<"AIza-key">>}, Responses).

user() -> user(<<"weather?">>).
user(Text) -> #{role => user, content => Text}.

input(Messages) -> #{model => ?MODEL, messages => Messages}.

tools() ->
    [
        #{
            name => <<"get_weather">>,
            parameters => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{<<"city">> => #{<<"type">> => <<"string">>}}
            }
        }
    ].

complete(Body, Input) ->
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, json:encode(Body))]),
    {thinwrap_llm:complete(C, Input), P}.

collect(Body, Input) ->
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    case thinwrap_llm:stream(C, Input, fun(D, A) -> [D | A] end, []) of
        {ok, Ds} -> {ok, lists:reverse(Ds)};
        E -> E
    end.

ok_body() ->
    #{
        <<"candidates">> => [
            #{
                <<"content">> => #{<<"parts">> => [#{<<"text">> => <<"ok">>}]},
                <<"finishReason">> => <<"STOP">>
            }
        ]
    }.

text(Ds) -> iolist_to_binary([maps:get(content_delta, D, <<>>) || D <- Ds]).

%% ---------------------------------------------------------------------------

gemini_complete_test() ->
    Body = #{
        <<"modelVersion">> => ?MODEL,
        <<"candidates">> => [
            #{
                <<"content">> => #{
                    <<"parts">> => [#{<<"text">> => <<"Hi ">>}, #{<<"text">> => <<"there">>}]
                },
                <<"finishReason">> => <<"STOP">>
            }
        ],
        <<"usageMetadata">> => #{
            <<"promptTokenCount">> => 9,
            <<"candidatesTokenCount">> => 12,
            <<"totalTokenCount">> => 21
        }
    },
    {{ok, R}, P} = complete(Body, #{
        model => ?MODEL,
        messages => [#{role => system, content => <<"Be terse">>}, user(<<"Hello">>)],
        temperature => 0.5,
        max_output_tokens => 256,
        stop => [<<"END">>],
        reasoning => #{effort => medium}
    }),
    Last = ?F:last(P),
    ?assertEqual(
        <<"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash:generateContent">>,
        maps:get(url, Last)
    ),
    ?assertEqual(
        <<"/v1beta/models/gemini-3.8-flash:generateContent">>,
        thinwrap_llm_fake_transport:path_of(Last)
    ),
    ?assertEqual(<<"AIza-key">>, thinwrap_llm_fake_transport:header(Last, <<"x-goog-api-key">>)),
    ?assertEqual(undefined, thinwrap_llm_fake_transport:header(Last, <<"Authorization">>)),
    Sent = thinwrap_llm_fake_transport:body_json(Last),
    ?assertEqual(
        #{<<"parts">> => [#{<<"text">> => <<"Be terse">>}]}, maps:get(<<"systemInstruction">>, Sent)
    ),
    ?assertEqual(
        [#{<<"role">> => <<"user">>, <<"parts">> => [#{<<"text">> => <<"Hello">>}]}],
        maps:get(<<"contents">>, Sent)
    ),
    ?assertEqual(
        #{
            <<"maxOutputTokens">> => 256,
            <<"temperature">> => 0.5,
            <<"stopSequences">> => [<<"END">>],
            <<"thinkingConfig">> => #{<<"thinkingBudget">> => 8192}
        },
        maps:get(<<"generationConfig">>, Sent)
    ),
    ?assertMatch(
        #{
            message := #{role := assistant, content := <<"Hi there">>, tool_calls := []},
            finish_reason := stop,
            model := ?MODEL,
            usage := #{input_tokens := 9, output_tokens := 12, total_tokens := 21}
        },
        R
    ),
    ?assertNot(maps:is_key(provider_metadata, maps:get(message, R))).

gemini_default_max_tokens_fallback_test() ->
    {C, P} = ?F:config(#{provider => gemini, api_key => <<"k">>, default_max_tokens => 1000}, [
        thinwrap_llm_fake_transport:resp(200, json:encode(ok_body()))
    ]),
    {ok, _} = thinwrap_llm:complete(C, input([user(<<"hi">>)])),
    ?assertMatch(
        #{<<"generationConfig">> := #{<<"maxOutputTokens">> := 1000}},
        thinwrap_llm_fake_transport:body_json(?F:last(P))
    ).

no_generation_config_when_nothing_is_set_test() ->
    {{ok, _}, P} = complete(ok_body(), input([user()])),
    ?assertNot(
        maps:is_key(<<"generationConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P)))
    ).

gemini_tool_call_test() ->
    Body = #{
        <<"candidates">> => [
            #{
                <<"content">> => #{
                    <<"parts">> => [
                        #{
                            <<"functionCall">> => #{
                                <<"name">> => <<"get_weather">>,
                                <<"args">> => #{<<"city">> => <<"SF">>}
                            }
                        }
                    ]
                }
            }
        ],
        <<"usageMetadata">> => #{
            <<"promptTokenCount">> => 5,
            <<"candidatesTokenCount">> => 7,
            <<"totalTokenCount">> => 12
        }
    },
    {{ok, R}, P} = complete(Body, #{
        model => ?MODEL,
        messages => [user()],
        tools => [
            #{
                name => <<"get_weather">>,
                description => <<"Look up weather">>,
                parameters => #{type => object, properties => #{city => #{type => string}}}
            }
        ],
        tool_choice => #{mode => required}
    }),
    Sent = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    [#{<<"functionDeclarations">> := [Decl]}] = maps:get(<<"tools">>, Sent),
    ?assertMatch(
        #{
            <<"name">> := <<"get_weather">>,
            <<"description">> := <<"Look up weather">>,
            <<"parametersJsonSchema">> := #{<<"type">> := <<"object">>}
        },
        Decl
    ),
    ?assertNot(maps:is_key(<<"parameters">>, Decl)),
    ?assertEqual(
        #{<<"functionCallingConfig">> => #{<<"mode">> => <<"ANY">>}},
        maps:get(<<"toolConfig">>, Sent)
    ),
    ?assertMatch(
        #{
            finish_reason := tool_calls,
            message := #{tool_calls := [#{id := <<"call_0">>, name := <<"get_weather">>} = TC]}
        } when not is_map_key(provider_metadata, TC),
        R
    ),
    [#{arguments := Args}] = maps:get(tool_calls, maps:get(message, R)),
    ?assertEqual(#{<<"city">> => <<"SF">>}, json:decode(Args)).

gemini_tool_result_and_function_config_test() ->
    {{ok, _}, P} = complete(
        #{
            <<"candidates">> => [
                #{
                    <<"content">> => #{<<"parts">> => [#{<<"text">> => <<"done">>}]},
                    <<"finishReason">> => <<"STOP">>
                }
            ]
        },
        #{
            model => ?MODEL,
            messages => [
                user(),
                #{
                    role => assistant,
                    tool_calls => [
                        #{
                            id => <<"call_abc">>,
                            name => <<"get_weather">>,
                            arguments => <<"{\"city\":\"SF\"}">>
                        }
                    ]
                },
                #{role => tool, tool_call_id => <<"call_abc">>, content => <<"{\"temp\":70}">>}
            ],
            tools => [#{name => <<"get_weather">>}],
            tool_choice => #{mode => function, function_name => <<"get_weather">>}
        }
    ),
    Sent = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    [_, Model, Tool] = maps:get(<<"contents">>, Sent),
    ?assertEqual(<<"model">>, maps:get(<<"role">>, Model)),
    ?assertMatch(
        #{<<"functionCall">> := #{<<"name">> := <<"get_weather">>}},
        lists:last(maps:get(<<"parts">>, Model))
    ),
    ?assertEqual(<<"user">>, maps:get(<<"role">>, Tool)),
    [#{<<"functionResponse">> := FR}] = maps:get(<<"parts">>, Tool),
    ?assertEqual(<<"get_weather">>, maps:get(<<"name">>, FR)),
    ?assertEqual(#{<<"result">> => #{<<"temp">> => 70}}, maps:get(<<"response">>, FR)),
    ?assertEqual(
        #{
            <<"functionCallingConfig">> => #{
                <<"mode">> => <<"ANY">>, <<"allowedFunctionNames">> => [<<"get_weather">>]
            }
        },
        maps:get(<<"toolConfig">>, Sent)
    ).

tool_choice_modes_and_tool_result_fallback_names_test() ->
    Modes = [{auto, <<"AUTO">>}, {none, <<"NONE">>}],
    [
        begin
            {{ok, _}, P} = complete(ok_body(), #{
                model => ?MODEL,
                messages => [user()],
                tools => [#{name => <<"f">>}],
                tool_choice => #{mode => Mode}
            }),
            ?assertEqual(
                #{<<"functionCallingConfig">> => #{<<"mode">> => Want}},
                maps:get(<<"toolConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P)))
            )
        end
     || {Mode, Want} <- Modes
    ],
    %% A tool result names its function from `name`, else the resolved id,
    %% else the raw id, else "tool"; a non-JSON result is sent as a string.
    {{ok, _}, P2} = complete(
        ok_body(),
        input([
            user(),
            #{role => tool, name => <<"named">>, tool_call_id => <<"x">>, content => <<"plain">>},
            user(<<"again">>),
            #{role => tool, tool_call_id => <<"unknown_id">>, content => <<"1">>},
            #{role => tool, content => <<"2">>}
        ])
    ),
    Contents = maps:get(<<"contents">>, thinwrap_llm_fake_transport:body_json(?F:last(P2))),
    Names = [
        maps:get(<<"name">>, FR)
     || #{<<"parts">> := Parts} <- Contents, #{<<"functionResponse">> := FR} <- Parts
    ],
    ?assertEqual([<<"named">>, <<"unknown_id">>, <<"tool">>], Names),
    [_, #{<<"parts">> := [#{<<"functionResponse">> := Plain}]} | _] = Contents,
    ?assertEqual(#{<<"result">> => <<"plain">>}, maps:get(<<"response">>, Plain)),
    ?assertEqual(<<"x">>, maps:get(<<"id">>, Plain)),
    %% consecutive tool results batch onto one user turn
    ?assertEqual(4, length(Contents)).

multimodal_parts_and_empty_message_test() ->
    {{ok, _}, P} = complete(
        ok_body(),
        input([
            #{
                role => user,
                content_parts => [
                    #{type => text, text => <<"look">>},
                    #{type => image, base64 => <<"QUJD">>, media_type => <<"image/png">>}
                ]
            },
            #{role => assistant}
        ])
    ),
    ?assertEqual(
        [
            #{
                <<"role">> => <<"user">>,
                <<"parts">> => [
                    #{<<"text">> => <<"look">>},
                    #{
                        <<"inlineData">> => #{
                            <<"mimeType">> => <<"image/png">>, <<"data">> => <<"QUJD">>
                        }
                    }
                ]
            },
            #{<<"role">> => <<"model">>, <<"parts">> => [#{<<"text">> => <<>>}]}
        ],
        maps:get(<<"contents">>, thinwrap_llm_fake_transport:body_json(?F:last(P)))
    ).

passthrough_and_config_headers_test() ->
    {C, P} = ?F:config(
        #{
            provider => gemini,
            api_key => <<"k">>,
            base_url => <<"https://proxy.example/v1beta/">>,
            headers => #{<<"x-cfg">> => <<"1">>}
        },
        [
            thinwrap_llm_fake_transport:resp(
                200, thinwrap_llm_fake_transport:sse([json:encode(ok_body())]), ?SSE
            )
        ]
    ),
    {ok, _} = thinwrap_llm:stream(
        C,
        (input([user()]))#{
            passthrough => #{
                query => #{<<"k2">> => <<"v 2">>},
                headers => #{<<"x-req">> => <<"y">>},
                body => #{<<"safetySettings">> => []}
            }
        },
        fun(_, A) -> A end,
        ok
    ),
    Last = ?F:last(P),
    ?assertEqual(
        <<"https://proxy.example/v1beta/models/gemini-3.8-flash:streamGenerateContent?alt=sse&k2=v+2">>,
        maps:get(url, Last)
    ),
    ?assertEqual(<<"1">>, thinwrap_llm_fake_transport:header(Last, <<"x-cfg">>)),
    ?assertEqual(<<"y">>, thinwrap_llm_fake_transport:header(Last, <<"x-req">>)),
    ?assertEqual([], maps:get(<<"safetySettings">>, thinwrap_llm_fake_transport:body_json(Last))).

model_id_is_percent_encoded_test() ->
    {{ok, _}, P} = complete(ok_body(), #{
        model => <<"tunedModels/a b">>,
        messages => [user()],
        passthrough => #{query => #{<<"q">> => <<"1">>}}
    }),
    ?assertEqual(
        <<"https://generativelanguage.googleapis.com/v1beta/models/tunedModels%2Fa%20b:generateContent?q=1">>,
        maps:get(url, ?F:last(P))
    ).

gemini_stream_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hel\"}]}}]}">>,
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"lo\"}]}}]}">>,
        <<"{\"candidates\":[{\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"totalTokenCount\":5}}">>
    ]),
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    {ok, Rev} = thinwrap_llm:stream(C, input([user(<<"hi">>)]), fun(D, A) -> [D | A] end, []),
    Ds = lists:reverse(Rev),
    ?assertEqual(<<"Hello">>, text(Ds)),
    ?assertEqual([stop], [F || #{finish_reason := F} <- Ds]),
    ?assertEqual([5], [T || #{usage := #{total_tokens := T}} <- Ds]),
    ?assertEqual(
        <<"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash:streamGenerateContent?alt=sse">>,
        maps:get(url, ?F:last(P))
    ).

gemini_stream_tool_call_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"lookup\",\"args\":{\"q\":\"x\"}}}]}}]}">>,
        <<"{\"candidates\":[{\"finishReason\":\"STOP\"}]}">>
    ]),
    {ok, Ds} = collect(Body, input([user(<<"hi">>)])),
    [TCD] = [T || #{tool_call_delta := T} <- Ds],
    ?assertMatch(#{index := 0, id := <<"call_0">>, function_name := <<"lookup">>}, TCD),
    ?assertNot(maps:is_key(provider_metadata, TCD)),
    ?assertEqual(#{<<"q">> => <<"x">>}, json:decode(maps:get(arguments_delta, TCD))),
    %% a terminal finishReason after a functionCall maps to tool_calls
    ?assertEqual([tool_calls], [F || #{finish_reason := F} <- Ds]).

gemini_rate_limit_retry_delay_test() ->
    Body = #{
        <<"error">> => #{
            <<"code">> => 429,
            <<"message">> => <<"Resource exhausted">>,
            <<"details">> => [
                #{
                    <<"@type">> => <<"type.googleapis.com/google.rpc.RetryInfo">>,
                    <<"retryDelay">> => <<"30s">>
                }
            ]
        }
    },
    {C, _} = chat([thinwrap_llm_fake_transport:resp(429, json:encode(Body))]),
    {error, E} = thinwrap_llm:complete(C, input([user()])),
    ?assertMatch(
        #{
            code := rate_limited,
            status := 429,
            provider_message := <<"Resource exhausted">>,
            cause := #{<<"retryAfterSeconds">> := 30}
        },
        E
    ).

retry_delay_rounds_and_header_wins_test() ->
    Body = fun(Delay) ->
        json:encode(#{
            <<"error">> => #{
                <<"code">> => 429,
                <<"details">> => [#{<<"x">> => 1}, #{<<"retryDelay">> => Delay}]
            }
        })
    end,
    {C1, _} = chat([thinwrap_llm_fake_transport:resp(429, Body(<<"2.5s">>))]),
    ?assertMatch(
        {error, #{cause := #{<<"retryAfterSeconds">> := 3}}},
        thinwrap_llm:complete(C1, input([user()]))
    ),
    {C2, _} = chat([
        thinwrap_llm_fake_transport:resp(429, Body(<<"7s">>), #{<<"Retry-After">> => <<"4">>})
    ]),
    ?assertMatch(
        {error, #{cause := #{<<"retryAfterSeconds">> := 4, <<"retryAfter">> := <<"4">>}}},
        thinwrap_llm:complete(C2, input([user()]))
    ),
    {C3, _} = chat([thinwrap_llm_fake_transport:resp(429, Body(<<"soon">>))]),
    {error, #{cause := Cause}} = thinwrap_llm:complete(C3, input([user()])),
    ?assertNot(maps:is_key(<<"retryAfterSeconds">>, Cause)).

gemini_context_length_error_test() ->
    Body = #{
        <<"error">> => #{
            <<"code">> => 400,
            <<"message">> =>
                <<"The input token count (1200000) exceeds the maximum number of tokens allowed (1048576).">>,
            <<"status">> => <<"INVALID_ARGUMENT">>
        }
    },
    {C, _} = chat([thinwrap_llm_fake_transport:resp(400, json:encode(Body))]),
    ?assertMatch(
        {error, #{code := context_length_exceeded}}, thinwrap_llm:complete(C, input([user()]))
    ).

gemini_auth_error_test() ->
    {C, _} = chat([
        thinwrap_llm_fake_transport:resp(401, <<"{\"error\":{\"message\":\"API key not valid\"}}">>)
    ]),
    ?assertMatch({error, #{code := auth_failed}}, thinwrap_llm:complete(C, input([user()]))).

gemini_stream_error_frame_raises_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hi\"}]}}]}">>,
        <<"{\"error\":{\"code\":500,\"message\":\"internal mid-stream\"}}">>
    ]),
    {C, _} = chat([thinwrap_llm_fake_transport:resp(200, Body, ?SSE)]),
    Self = self(),
    Result = thinwrap_llm:stream(
        C,
        input([user()]),
        fun(D, A) ->
            Self ! {d, D},
            A
        end,
        ok
    ),
    receive
        {d, First} -> ?assertEqual(<<"Hi">>, maps:get(content_delta, First))
    after 0 -> ?assert(false)
    end,
    ?assertMatch(
        {error, #{
            code := provider_unavailable,
            status := 200,
            provider_message := <<"internal mid-stream">>
        }},
        Result
    ).

gemini_requires_gemini_config_test() ->
    ?assertError(
        {badarg, _},
        thinwrap_llm:complete(
            #{provider => gemini, api_key => <<"k">>, region => <<"x">>}, input([user()])
        )
    ),
    ?assertError({badarg, _}, thinwrap_llm:complete(#{provider => gemini}, input([user()]))).

gemini_prompt_block_content_filter_test() ->
    Body = #{
        <<"promptFeedback">> => #{<<"blockReason">> => <<"SAFETY">>},
        <<"usageMetadata">> => #{<<"promptTokenCount">> => 9, <<"totalTokenCount">> => 9}
    },
    {{ok, R}, _} = complete(Body, input([user(<<"x">>)])),
    ?assertMatch(#{finish_reason := content_filter, message := #{content := <<>>}}, R).

gemini_stream_prompt_block_content_filter_test() ->
    Body = <<"data: {\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}\n\n\n">>,
    {ok, Ds} = collect(Body, input([user(<<"x">>)])),
    ?assertEqual([content_filter], [F || #{finish_reason := F} <- Ds]).

gemini_thought_parts_excluded_from_content_test() ->
    Body = #{
        <<"candidates">> => [
            #{
                <<"content">> => #{
                    <<"parts">> => [
                        #{<<"text">> => <<"secret cot">>, <<"thought">> => true},
                        #{<<"text">> => <<"the answer">>}
                    ]
                },
                <<"finishReason">> => <<"STOP">>
            }
        ],
        <<"usageMetadata">> => #{}
    },
    {{ok, R}, _} = complete(Body, #{model => <<"gemini-3-pro">>, messages => [user(<<"hi">>)]}),
    ?assertMatch(#{message := #{content := <<"the answer">>}}, R).

gemini_stream_thought_parts_excluded_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"cot\",\"thought\":true},{\"text\":\"ans\"}]},\"finishReason\":\"STOP\"}]}">>
    ]),
    {ok, Ds} = collect(Body, input([user()])),
    ?assertEqual(<<"ans">>, text(Ds)).

gemini_200_without_candidates_or_block_reason_is_empty_body_error_test_() ->
    [
        ?_test(begin
            {C, _} = chat([thinwrap_llm_fake_transport:resp(200, B)]),
            ?assertMatch(
                {error, #{code := provider_unavailable, status := 200}},
                thinwrap_llm:complete(C, input([user(<<"x">>)]))
            )
        end)
     || B <- [
            <<"{}">>,
            <<"{\"usageMetadata\":{\"promptTokenCount\":3},\"modelVersion\":\"gemini-3.8-flash\"}">>,
            <<"{\"promptFeedback\":{}}">>,
            <<>>
        ]
    ].

%% ---------------------------------------------------------------------------
%% provider_metadata: thought signatures

signature_and_vendor_id_captured_on_tool_call_test() ->
    Body = #{
        <<"candidates">> => [
            #{
                <<"content">> => #{
                    <<"role">> => <<"model">>,
                    <<"parts">> => [
                        #{
                            <<"functionCall">> => #{
                                <<"id">> => <<"fc-1">>,
                                <<"name">> => <<"get_weather">>,
                                <<"args">> => #{<<"city">> => <<"Paris">>}
                            },
                            <<"thoughtSignature">> => <<"sig-1">>
                        },
                        #{
                            <<"functionCall">> => #{
                                <<"id">> => <<"fc-2">>,
                                <<"name">> => <<"get_weather">>,
                                <<"args">> => #{<<"city">> => <<"Rome">>}
                            }
                        }
                    ]
                },
                <<"finishReason">> => <<"STOP">>
            }
        ]
    },
    {{ok, R}, _} = complete(Body, (input([user()]))#{tools => tools()}),
    #{message := #{tool_calls := [A, B]} = Msg} = R,
    ?assertMatch(
        #{
            id := <<"fc-1">>,
            provider_metadata := #{<<"gemini">> := #{<<"thoughtSignature">> := <<"sig-1">>}}
        },
        A
    ),
    %% Parallel calls: only the first part is signed.
    ?assertEqual(<<"fc-2">>, maps:get(id, B)),
    ?assertNot(maps:is_key(provider_metadata, B)),
    ?assertNot(maps:is_key(provider_metadata, Msg)).

synthetic_ids_are_index_based_for_parallel_same_name_calls_test() ->
    Body = #{
        <<"candidates">> => [
            #{
                <<"content">> => #{
                    <<"parts">> => [
                        #{
                            <<"functionCall">> => #{
                                <<"name">> => <<"get_weather">>,
                                <<"args">> => #{<<"city">> => <<"Paris">>}
                            },
                            <<"thoughtSignature">> => <<"s">>
                        },
                        #{
                            <<"functionCall">> => #{
                                <<"name">> => <<"get_weather">>,
                                <<"args">> => #{<<"city">> => <<"Rome">>}
                            }
                        }
                    ]
                }
            }
        ]
    },
    {{ok, R}, _} = complete(Body, (input([user()]))#{tools => tools()}),
    ?assertEqual(
        [<<"call_0">>, <<"call_1">>], [
            Id
         || #{id := Id} <- maps:get(tool_calls, maps:get(message, R))
        ]
    ).

stream_captures_signature_and_index_based_ids_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"Paris\"}},\"thoughtSignature\":\"sig-s\"}]}}]}">>,
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"Rome\"}}}]},\"finishReason\":\"STOP\"}]}">>
    ]),
    {ok, Ds} = collect(Body, (input([user()]))#{tools => tools()}),
    TCDs = [T || #{tool_call_delta := T} <- Ds],
    ?assertEqual([{0, <<"call_0">>}, {1, <<"call_1">>}], [
        {I, Id}
     || #{index := I, id := Id} <- TCDs
    ]),
    [First, Second] = TCDs,
    ?assertEqual(
        #{<<"gemini">> => #{<<"thoughtSignature">> => <<"sig-s">>}},
        maps:get(provider_metadata, First)
    ),
    ?assertNot(maps:is_key(provider_metadata, Second)).

replayed_model_turn(ToolCalls) ->
    Msgs =
        [user(), #{role => assistant, tool_calls => ToolCalls}] ++
            [
                #{role => tool, tool_call_id => Id, content => <<"{\"temp\":20}">>}
             || #{id := Id} <- ToolCalls
            ],
    {{ok, _}, P} = complete(ok_body(), (input(Msgs))#{tools => tools()}),
    maps:get(<<"contents">>, thinwrap_llm_fake_transport:body_json(?F:last(P))).

replay_puts_stored_signature_back_and_echoes_vendor_ids_test() ->
    Contents = replayed_model_turn([
        #{
            id => <<"fc-1">>,
            name => <<"get_weather">>,
            arguments => <<"{\"city\":\"Paris\"}">>,
            provider_metadata => #{<<"gemini">> => #{<<"thoughtSignature">> => <<"sig-1">>}}
        },
        #{id => <<"fc-2">>, name => <<"get_weather">>, arguments => <<"{\"city\":\"Rome\"}">>}
    ]),
    ?assertEqual(
        #{
            <<"role">> => <<"model">>,
            <<"parts">> => [
                #{
                    <<"functionCall">> => #{
                        <<"name">> => <<"get_weather">>,
                        <<"args">> => #{<<"city">> => <<"Paris">>},
                        <<"id">> => <<"fc-1">>
                    },
                    <<"thoughtSignature">> => <<"sig-1">>
                },
                #{
                    <<"functionCall">> => #{
                        <<"name">> => <<"get_weather">>,
                        <<"args">> => #{<<"city">> => <<"Rome">>},
                        <<"id">> => <<"fc-2">>
                    }
                }
            ]
        },
        lists:nth(2, Contents)
    ),
    Responses = [
        FR
     || #{<<"functionResponse">> := FR} <- maps:get(<<"parts">>, lists:nth(3, Contents))
    ],
    ?assertEqual(
        [{<<"get_weather">>, <<"fc-1">>}, {<<"get_weather">>, <<"fc-2">>}],
        [{N, Id} || #{<<"name">> := N, <<"id">> := Id} <- Responses]
    ).

replay_without_signature_uses_dummy_on_first_call_only_test() ->
    Contents = replayed_model_turn([
        #{id => <<"call_0">>, name => <<"get_weather">>, arguments => <<"{\"city\":\"Paris\"}">>},
        #{id => <<"call_1">>, name => <<"get_weather">>, arguments => <<"{\"city\":\"Rome\"}">>}
    ]),
    [P0, P1] = maps:get(<<"parts">>, lists:nth(2, Contents)),
    ?assertEqual(<<"skip_thought_signature_validator">>, maps:get(<<"thoughtSignature">>, P0)),
    ?assertNot(maps:is_key(<<"thoughtSignature">>, P1)),
    %% Synthetic ids are never sent back: Gemini only knows the ids it issued.
    ?assertNot(maps:is_key(<<"id">>, maps:get(<<"functionCall">>, P0))),
    ?assertNot(maps:is_key(<<"id">>, maps:get(<<"functionCall">>, P1))),
    ?assert(
        lists:all(
            fun(#{<<"functionResponse">> := FR}) -> not maps:is_key(<<"id">>, FR) end,
            maps:get(<<"parts">>, lists:nth(3, Contents))
        )
    ).

dummy_is_per_model_turn_test() ->
    Msgs = [
        user(),
        #{
            role => assistant,
            tool_calls => [
                #{
                    id => <<"fc-1">>,
                    name => <<"get_weather">>,
                    arguments => <<"{}">>,
                    provider_metadata => #{<<"gemini">> => #{<<"thoughtSignature">> => <<"sig-1">>}}
                }
            ]
        },
        #{role => tool, tool_call_id => <<"fc-1">>, content => <<"sunny">>},
        #{
            role => assistant,
            content => <<"Next.">>,
            tool_calls => [#{id => <<"call_0">>, name => <<"get_weather">>, arguments => <<"{}">>}]
        },
        #{role => tool, tool_call_id => <<"call_0">>, content => <<"rainy">>}
    ],
    {{ok, _}, P} = complete(ok_body(), (input(Msgs))#{tools => tools()}),
    Contents = maps:get(<<"contents">>, thinwrap_llm_fake_transport:body_json(?F:last(P))),
    ?assertMatch(
        #{<<"parts">> := [#{<<"thoughtSignature">> := <<"sig-1">>}]}, lists:nth(2, Contents)
    ),
    #{<<"parts">> := [Text, Call]} = lists:nth(4, Contents),
    ?assertEqual(#{<<"text">> => <<"Next.">>}, Text),
    ?assertEqual(<<"skip_thought_signature_validator">>, maps:get(<<"thoughtSignature">>, Call)).

replay_ignores_foreign_provider_metadata_test() ->
    Contents = replayed_model_turn([
        #{
            id => <<"toolu_01">>,
            name => <<"get_weather">>,
            arguments => <<"{}">>,
            provider_metadata => #{<<"anthropic">> => #{<<"thoughtSignature">> => <<"not-gemini">>}}
        }
    ]),
    [Part] = maps:get(<<"parts">>, lists:nth(2, Contents)),
    ?assertEqual(<<"skip_thought_signature_validator">>, maps:get(<<"thoughtSignature">>, Part)),
    ?assertEqual(nomatch, binary:match(iolist_to_binary(json:encode(Contents)), <<"not-gemini">>)).

round_trip_from_result_to_next_request_test() ->
    Body = #{
        <<"candidates">> => [
            #{
                <<"content">> => #{
                    <<"parts">> => [
                        #{<<"text">> => <<"Let me check.">>},
                        #{
                            <<"functionCall">> => #{
                                <<"name">> => <<"get_weather">>,
                                <<"args">> => #{<<"city">> => <<"Paris">>}
                            },
                            <<"thoughtSignature">> => <<"sig-rt">>
                        }
                    ]
                },
                <<"finishReason">> => <<"STOP">>
            }
        ]
    },
    {C, P} = chat([thinwrap_llm_fake_transport:resp(200, json:encode(Body))]),
    {ok, #{message := Msg}} = thinwrap_llm:complete(C, (input([user()]))#{tools => tools()}),
    #{content := Content, tool_calls := [#{id := Id} | _] = TCs} = Msg,
    {ok, _} = thinwrap_llm:complete(
        C,
        (input([
            user(),
            maps:merge(
                #{role => assistant, content => Content, tool_calls => TCs},
                maps:with([provider_metadata], Msg)
            ),
            #{role => tool, tool_call_id => Id, content => <<"sunny">>}
        ]))#{
            tools => tools()
        }
    ),
    #{<<"parts">> := [_, Call]} = lists:nth(
        2, maps:get(<<"contents">>, thinwrap_llm_fake_transport:body_json(?F:last(P)))
    ),
    ?assertEqual(<<"sig-rt">>, maps:get(<<"thoughtSignature">>, Call)).

%% ---------------------------------------------------------------------------
%% schemas, usage, finish reasons, errors

json_schema_response_format_uses_response_json_schema_test() ->
    Schema = #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{<<"a">> => #{<<"type">> => <<"string">>}},
        <<"additionalProperties">> => false
    },
    {{ok, _}, P} = complete(ok_body(), (input([user()]))#{
        response_format => #{
            type => json_schema, json_schema_name => <<"x">>, json_schema => Schema
        }
    }),
    GC = maps:get(<<"generationConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P))),
    ?assertEqual(Schema, maps:get(<<"responseJsonSchema">>, GC)),
    ?assertNot(maps:is_key(<<"responseSchema">>, GC)),
    ?assertEqual(<<"application/json">>, maps:get(<<"responseMimeType">>, GC)),
    {{ok, _}, P2} = complete(ok_body(), (input([user()]))#{
        response_format => #{type => json_object}
    }),
    ?assertEqual(
        #{<<"responseMimeType">> => <<"application/json">>},
        maps:get(<<"generationConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P2)))
    ),
    {{ok, _}, P3} = complete(ok_body(), (input([user()]))#{response_format => #{type => text}}),
    ?assertNot(
        maps:is_key(<<"generationConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P3)))
    ).

effort_budgets_and_unknown_effort_falls_back_to_medium_test_() ->
    [
        ?_test(begin
            {{ok, _}, P} = complete(ok_body(), (input([user()]))#{reasoning => #{effort => Effort}}),
            ?assertEqual(
                #{<<"thinkingBudget">> => Want},
                maps:get(
                    <<"thinkingConfig">>,
                    maps:get(
                        <<"generationConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P))
                    )
                )
            )
        end)
     || {Effort, Want} <- [
            {low, 1024}, {medium, 8192}, {high, 24576}, {<<"extreme">>, 8192}, {<<>>, 8192}
        ]
    ].

usage_counts_thought_tokens_as_output_test() ->
    Body = (ok_body())#{
        <<"usageMetadata">> => #{
            <<"promptTokenCount">> => 10,
            <<"candidatesTokenCount">> => 5,
            <<"thoughtsTokenCount">> => 40,
            <<"totalTokenCount">> => 55
        }
    },
    {{ok, #{usage := U}}, _} = complete(Body, input([user()])),
    ?assertEqual(#{input_tokens => 10, output_tokens => 45, total_tokens => 55}, U).

stream_reports_cumulative_usage_once_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hel\"}]}}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":1,\"totalTokenCount\":11}}">>,
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"lo\"}]}}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":2,\"totalTokenCount\":12}}">>,
        <<"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"!\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":3,\"thoughtsTokenCount\":7,\"totalTokenCount\":20}}">>
    ]),
    {ok, Ds} = collect(Body, input([user()])),
    Usages = [U || #{usage := U} <- Ds],
    ?assertEqual([#{input_tokens => 10, output_tokens => 10, total_tokens => 20}], Usages),
    ?assertMatch(#{finish_reason := stop, usage := _, raw := #{}}, lists:last(Ds)),
    ?assertEqual(<<"Hello!">>, text(Ds)).

finish_reason_mapping_test_() ->
    [
        ?_test(begin
            Body = #{
                <<"candidates">> => [
                    #{
                        <<"content">> => #{<<"parts">> => [#{<<"text">> => <<"x">>}]},
                        <<"finishReason">> => Reason
                    }
                ]
            },
            {{ok, R}, _} = complete(Body, input([user()])),
            ?assertEqual(Want, maps:get(finish_reason, R))
        end)
     || {Reason, Want} <- [
            {<<"MALFORMED_FUNCTION_CALL">>, unknown},
            {<<"UNEXPECTED_TOOL_CALL">>, unknown},
            {<<"IMAGE_SAFETY">>, content_filter},
            {<<"OTHER">>, unknown},
            {<<"CONTINUATION">>, length},
            {<<"LANGUAGE">>, unknown},
            {<<"IMAGE_RECITATION">>, content_filter},
            {<<"NO_IMAGE">>, unknown},
            {<<"MAX_TOKENS">>, length},
            {<<"SAFETY">>, content_filter},
            {<<"RECITATION">>, content_filter},
            {<<"BLOCKLIST">>, content_filter},
            {<<"PROHIBITED_CONTENT">>, content_filter},
            {<<"SPII">>, content_filter},
            {<<"IMAGE_PROHIBITED_CONTENT">>, content_filter}
        ]
    ].

api_key_invalid() ->
    #{
        <<"code">> => 400,
        <<"message">> => <<"API key not valid. Please pass a valid API key.">>,
        <<"status">> => <<"INVALID_ARGUMENT">>,
        <<"details">> => [
            #{
                <<"@type">> => <<"type.googleapis.com/google.rpc.ErrorInfo">>,
                <<"reason">> => <<"API_KEY_INVALID">>,
                <<"domain">> => <<"googleapis.com">>,
                <<"metadata">> => #{<<"service">> => <<"generativelanguage.googleapis.com">>}
            },
            #{
                <<"@type">> => <<"type.googleapis.com/google.rpc.LocalizedMessage">>,
                <<"locale">> => <<"en-US">>,
                <<"message">> => <<"API key not valid. Please pass a valid API key.">>
            }
        ]
    }.

error_classification_test_() ->
    [
        ?_test(begin
            {C, _} = chat([
                thinwrap_llm_fake_transport:resp(Status, json:encode(#{<<"error">> => Err}))
            ]),
            ?assertMatch(
                {error, #{code := Want, status := Status}},
                thinwrap_llm:complete(C, input([user()]))
            )
        end)
     || {Status, Err, Want} <- [
            {400, api_key_invalid(), auth_failed},
            {400,
                #{
                    <<"code">> => 400,
                    <<"message">> =>
                        <<"The input token count (1200000) exceeds the maximum number of tokens allowed (1048576).">>,
                    <<"status">> => <<"INVALID_ARGUMENT">>
                },
                context_length_exceeded},
            {400,
                #{
                    <<"code">> => 400,
                    <<"message">> =>
                        <<"Invalid JSON payload received. Unknown name \"context\" at 'generation_config'.">>,
                    <<"status">> => <<"INVALID_ARGUMENT">>
                },
                invalid_request},
            {400,
                #{
                    <<"code">> => 400,
                    <<"message">> =>
                        <<"Function call is missing a thought_signature in functionCall parts.">>,
                    <<"status">> => <<"INVALID_ARGUMENT">>
                },
                invalid_request},
            {402,
                #{
                    <<"code">> => 402,
                    <<"message">> => <<"Your Prepay credit balance is depleted.">>,
                    <<"status">> => <<"RESOURCE_EXHAUSTED">>
                },
                rate_limited},
            {504,
                #{
                    <<"code">> => 504,
                    <<"message">> => <<"Deadline exceeded">>,
                    <<"status">> => <<"DEADLINE_EXCEEDED">>
                },
                provider_unavailable},
            {408, #{<<"code">> => 408, <<"message">> => <<"Request timeout">>},
                provider_unavailable},
            {409, #{<<"code">> => 409, <<"message">> => <<"Aborted">>}, invalid_request},
            {400, #{<<"code">> => 400, <<"message">> => <<"The input token count is required">>},
                invalid_request},
            {403, #{<<"message">> => <<"denied">>}, auth_failed}
        ]
    ].

stream_error_frame_api_key_invalid_without_code_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        json:encode(#{<<"error">> => maps:remove(<<"code">>, api_key_invalid())})
    ]),
    ?assertMatch({error, #{code := auth_failed}}, collect(Body, input([user()]))).

stream_error_frame_without_code_or_known_status_is_unknown_test() ->
    Body = <<"data: {\"error\":{\"message\":\"odd\",\"status\":\"SOMETHING_NEW\"}}\n\n">>,
    ?assertMatch({error, #{code := unknown, status := 200}}, collect(Body, input([user()]))).

stream_error_frame_classified_by_status_string_test_() ->
    [
        ?_assertMatch(
            {error, #{code := Want, status := 200}},
            collect(
                thinwrap_llm_fake_transport:sse([
                    json:encode(#{<<"error">> => #{<<"message">> => Msg, <<"status">> => Status}})
                ]),
                input([user()])
            )
        )
     || {Status, Msg, Want} <- [
            {<<"RESOURCE_EXHAUSTED">>, <<"q">>, rate_limited},
            {<<"unauthenticated">>, <<"no">>, auth_failed},
            {<<"PERMISSION_DENIED">>, <<"no">>, auth_failed},
            {<<"INVALID_ARGUMENT">>, <<"bad">>, invalid_request},
            {<<"OUT_OF_RANGE">>,
                <<"The input token count (9) exceeds the maximum number of tokens allowed (1).">>,
                context_length_exceeded},
            {<<"UNAVAILABLE">>, <<"later">>, provider_unavailable},
            {<<"ABORTED">>, <<"later">>, provider_unavailable}
        ]
    ].

stream_error_frame_with_code_is_classified_by_it_test() ->
    Body = thinwrap_llm_fake_transport:sse([
        <<"{\"error\":{\"code\":429,\"message\":\"slow\",\"status\":\"RESOURCE_EXHAUSTED\"}}">>
    ]),
    ?assertMatch({error, #{code := rate_limited, status := 200}}, collect(Body, input([user()]))).

%% ---------------------------------------------------------------------------
%% review-5

tool_result_name_resolves_against_the_preceding_turn_test() ->
    Msgs = [
        user(),
        #{
            role => assistant,
            tool_calls => [#{id => <<"call_0">>, name => <<"get_weather">>, arguments => <<"{}">>}]
        },
        #{role => tool, tool_call_id => <<"call_0">>, content => <<"{\"temp\":20}">>},
        #{
            role => assistant,
            tool_calls => [#{id => <<"call_0">>, name => <<"get_time">>, arguments => <<"{}">>}]
        },
        #{role => tool, tool_call_id => <<"call_0">>, content => <<"{\"t\":20}">>}
    ],
    {{ok, _}, P} = complete(ok_body(), (input(Msgs))#{tools => tools()}),
    Contents = maps:get(<<"contents">>, thinwrap_llm_fake_transport:body_json(?F:last(P))),
    Name = fun(N) ->
        #{<<"parts">> := [#{<<"functionResponse">> := #{<<"name">> := X}}]} = lists:nth(
            N, Contents
        ),
        X
    end,
    ?assertEqual(<<"get_weather">>, Name(3)),
    ?assertEqual(<<"get_time">>, Name(5)).

usage_counts_tool_use_prompt_tokens_as_input_test() ->
    Body = (ok_body())#{
        <<"usageMetadata">> => #{
            <<"promptTokenCount">> => 10,
            <<"toolUsePromptTokenCount">> => 4,
            <<"candidatesTokenCount">> => 5,
            <<"thoughtsTokenCount">> => 6,
            <<"totalTokenCount">> => 25
        }
    },
    {{ok, #{usage := U}}, _} = complete(Body, input([user()])),
    ?assertEqual(#{input_tokens => 14, output_tokens => 11, total_tokens => 25}, U).

system_messages_are_joined_test() ->
    {{ok, _}, P} = complete(
        ok_body(),
        input([
            #{role => system, content => <<"a">>},
            #{role => system, content_parts => [#{type => text, text => <<"b">>}]},
            user()
        ])
    ),
    ?assertEqual(
        #{<<"parts">> => [#{<<"text">> => <<"a\n\nb">>}]},
        maps:get(<<"systemInstruction">>, thinwrap_llm_fake_transport:body_json(?F:last(P)))
    ).
