-module(thinwrap_llm_bedrock_tests).

%% Ported from llm-py tests/test_bedrock.py (plus test_sigv4_structure from
%% test_core.py): Converse request build + SigV4 signing, normalized result
%% parse, tool use, session-token signing, the non-incremental streaming
%% fallback, and error mapping.

-include_lib("eunit/include/eunit.hrl").

-define(F, thinwrap_llm_fake_transport).
-define(MODEL, <<"anthropic.claude-sonnet-4-5-20250929-v1:0">>).
-define(HOST, <<"bedrock-runtime.us-east-1.amazonaws.com">>).
-define(OK,
    <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"x\"}]}},\"stopReason\":\"end_turn\"}">>
).

cfg(Responses) -> cfg(#{}, Responses).

cfg(Extra, Responses) ->
    ?F:config(
        maps:merge(
            #{
                provider => bedrock,
                region => <<"us-east-1">>,
                access_key_id => <<"AKIDEXAMPLE">>,
                secret_access_key => <<"wSecret">>
            },
            Extra
        ),
        Responses
    ).

user(Text) -> #{model => ?MODEL, messages => [#{role => user, content => Text}]}.

q(B) -> thinwrap_llm_util:rfc3986(B).

single() -> <<"/model/", (q(?MODEL))/binary, "/converse">>.
double() -> <<"/model/", (q(q(?MODEL)))/binary, "/converse">>.

%% Recompute the SigV4 Authorization over EXACTLY the bytes/date the connector
%% used, varying only the path / content type, so a test can assert which
%% canonical inputs the emitted signature actually committed to.
resign(Req, Path, ContentType) ->
    <<Y:4/binary, Mo:2/binary, D:2/binary, "T", H:2/binary, Mi:2/binary, S:2/binary, "Z">> =
        thinwrap_llm_fake_transport:header(Req, <<"X-Amz-Date">>),
    [Yi, Moi, Di, Hi, Mii, Si] = [binary_to_integer(X) || X <- [Y, Mo, D, H, Mi, S]],
    Now = calendar:datetime_to_gregorian_seconds({{Yi, Moi, Di}, {Hi, Mii, Si}}) - 62167219200,
    maps:get(
        <<"Authorization">>,
        thinwrap_llm_sigv4:sign(#{
            method => <<"POST">>,
            service => <<"bedrock">>,
            region => <<"us-east-1">>,
            host => ?HOST,
            path => Path,
            payload => maps:get(body, Req),
            access_key_id => <<"AKIDEXAMPLE">>,
            secret_access_key => <<"wSecret">>,
            signed => #{<<"Content-Type">> => ContentType},
            now => Now
        })
    ).

signed_headers(Auth) ->
    [_, After] = binary:split(Auth, <<"SignedHeaders=">>),
    [SH | _] = binary:split(After, <<",">>),
    SH.

has(Hay, Needle) -> binary:match(Hay, Needle) =/= nomatch.

%% ---------------------------------------------------------------------------

sigv4_structure_test() ->
    P = #{
        method => <<"POST">>,
        service => <<"bedrock">>,
        region => <<"us-east-1">>,
        host => ?HOST,
        path => <<"/model/m/converse">>,
        payload => <<"{\"x\":1}">>,
        access_key_id => <<"AKIDEXAMPLE">>,
        secret_access_key => <<"wSecret">>,
        signed => #{<<"Content-Type">> => <<"application/json">>},
        now => calendar:datetime_to_gregorian_seconds({{2026, 1, 2}, {3, 4, 5}}) - 62167219200
    },
    H1 = thinwrap_llm_sigv4:sign(P),
    H2 = thinwrap_llm_sigv4:sign(P),
    Auth = maps:get(<<"Authorization">>, H1),
    ?assertMatch(
        <<"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260102/us-east-1/bedrock/aws4_request",
            _/binary>>,
        Auth
    ),
    ?assert(has(Auth, <<"content-type;host;x-amz-content-sha256;x-amz-date">>)),
    ?assertEqual(H1, H2),
    [_, Sig] = binary:split(Auth, <<"Signature=">>),
    ?assertMatch({match, _}, re:run(Sig, "^[0-9a-f]{64}$")),
    ?assertEqual(<<"20260102T030405Z">>, maps:get(<<"X-Amz-Date">>, H1)),
    ?assertEqual(?HOST, maps:get(<<"Host">>, H1)).

sigv4_canonical_query_is_sorted_and_rfc3986_test() ->
    ?assertEqual(
        <<"a=1&b%20c=d%2Fe~">>,
        thinwrap_llm_sigv4:canonical_query(#{<<"b c">> => <<"d/e~">>, <<"a">> => <<"1">>})
    ).

complete_signing_and_result_test() ->
    Body =
        <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"Hi \"},{\"text\":\"there\"}]}},\"stopReason\":\"end_turn\",\"usage\":{\"inputTokens\":9,\"outputTokens\":12,\"totalTokens\":21}}">>,
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, R} = thinwrap_llm:complete(C, #{
        model => ?MODEL,
        messages => [
            #{role => system, content => <<"be brief">>}, #{role => user, content => <<"Hello">>}
        ],
        temperature => 0.5,
        top_p => 0.9,
        max_output_tokens => 256,
        stop => [<<"STOP">>]
    }),
    ?assertMatch(
        #{
            message := #{content := <<"Hi there">>, tool_calls := []},
            finish_reason := stop,
            usage := #{input_tokens := 9, output_tokens := 12, total_tokens := 21},
            model := ?MODEL
        },
        R
    ),
    Req = ?F:last(P),
    %% Percent-encoded model id in the Converse path.
    ?assertEqual(
        <<"/model/anthropic.claude-sonnet-4-5-20250929-v1%3A0/converse">>,
        thinwrap_llm_fake_transport:path_of(Req)
    ),
    ?assertEqual(<<"https://", ?HOST/binary, (single())/binary>>, maps:get(url, Req)),
    Auth = thinwrap_llm_fake_transport:header(Req, <<"Authorization">>),
    {match, [Day]} = re:run(
        Auth,
        "Credential=AKIDEXAMPLE/(\\d{8})/us-east-1/bedrock/aws4_request",
        [{capture, all_but_first, binary}]
    ),
    SH = signed_headers(Auth),
    [
        ?assert(has(SH, N))
     || N <- [<<"content-type">>, <<"host">>, <<"x-amz-content-sha256">>, <<"x-amz-date">>]
    ],
    [_, Sig] = binary:split(Auth, <<"Signature=">>),
    ?assertMatch({match, _}, re:run(Sig, "^[0-9a-f]{64}$")),
    AmzDate = thinwrap_llm_fake_transport:header(Req, <<"X-Amz-Date">>),
    ?assertMatch({match, _}, re:run(AmzDate, "^\\d{8}T\\d{6}Z$")),
    ?assertEqual(Day, binary:part(AmzDate, 0, 8)),
    %% Host is NOT attached as a request header (the transport sets it).
    ?assertEqual(undefined, thinwrap_llm_fake_transport:header(Req, <<"Host">>)),
    ?assertEqual(<<"application/json">>, maps:get(<<"Content-Type">>, maps:get(headers, Req))),
    Sent = thinwrap_llm_fake_transport:body_json(Req),
    ?assertEqual([#{<<"text">> => <<"be brief">>}], maps:get(<<"system">>, Sent)),
    ?assertEqual(
        [#{<<"role">> => <<"user">>, <<"content">> => [#{<<"text">> => <<"Hello">>}]}],
        maps:get(<<"messages">>, Sent)
    ),
    ?assertEqual(
        #{
            <<"maxTokens">> => 256,
            <<"temperature">> => 0.5,
            <<"topP">> => 0.9,
            <<"stopSequences">> => [<<"STOP">>]
        },
        maps:get(<<"inferenceConfig">>, Sent)
    ).

default_max_tokens_and_config_default_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, user(<<"hi">>)),
    ?assertMatch(
        #{<<"inferenceConfig">> := #{<<"maxTokens">> := 4096}},
        thinwrap_llm_fake_transport:body_json(?F:last(P))
    ),
    {C2, P2} = cfg(#{default_max_tokens => 1000}, [thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C2, user(<<"hi">>)),
    ?assertMatch(
        #{<<"inferenceConfig">> := #{<<"maxTokens">> := 1000}},
        thinwrap_llm_fake_transport:body_json(?F:last(P2))
    ).

complete_tool_use_test() ->
    Body =
        <<"{\"output\":{\"message\":{\"content\":[{\"toolUse\":{\"toolUseId\":\"tu_1\",\"name\":\"get_weather\",\"input\":{\"city\":\"Paris\"}}}]}},\"stopReason\":\"tool_use\",\"usage\":{\"inputTokens\":15,\"outputTokens\":7}}">>,
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
    Params = #{type => object, properties => #{city => #{type => string}}},
    {ok, R} = thinwrap_llm:complete(C, (user(<<"weather?">>))#{
        tools => [
            #{name => <<"get_weather">>, description => <<"Get weather">>, parameters => Params}
        ],
        tool_choice => #{mode => required}
    }),
    ?assertMatch(
        #{finish_reason := tool_calls, message := #{content := <<>>, tool_calls := [_]}}, R
    ),
    [TC] = maps:get(tool_calls, maps:get(message, R)),
    ?assertMatch(#{id := <<"tu_1">>, name := <<"get_weather">>}, TC),
    ?assertEqual(#{<<"city">> => <<"Paris">>}, json:decode(maps:get(arguments, TC))),
    %% totalTokens absent → derived.
    ?assertMatch(#{usage := #{total_tokens := 22}}, R),
    Sent = thinwrap_llm_fake_transport:body_json(?F:last(P)),
    ?assertEqual(
        [
            #{
                <<"toolSpec">> => #{
                    <<"name">> => <<"get_weather">>,
                    <<"description">> => <<"Get weather">>,
                    <<"inputSchema">> => #{
                        <<"json">> => #{
                            <<"type">> => <<"object">>,
                            <<"properties">> => #{<<"city">> => #{<<"type">> => <<"string">>}}
                        }
                    }
                }
            }
        ],
        maps:get(<<"tools">>, maps:get(<<"toolConfig">>, Sent))
    ),
    ?assertEqual(#{<<"any">> => #{}}, maps:get(<<"toolChoice">>, maps:get(<<"toolConfig">>, Sent))).

tool_choice_mapping_test_() ->
    [
        ?_test(begin
            {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
            {ok, _} = thinwrap_llm:complete(C, (user(<<"x">>))#{
                tools => [#{name => <<"f">>}], tool_choice => TC
            }),
            ToolConfig = maps:get(
                <<"toolConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P))
            ),
            ?assertEqual(
                [
                    #{
                        <<"toolSpec">> => #{
                            <<"name">> => <<"f">>,
                            <<"inputSchema">> => #{
                                <<"json">> => #{<<"type">> => <<"object">>, <<"properties">> => #{}}
                            }
                        }
                    }
                ],
                maps:get(<<"tools">>, ToolConfig)
            ),
            ?assertEqual(Want, maps:get(<<"toolChoice">>, ToolConfig))
        end)
     || {TC, Want} <- [
            {#{mode => auto}, #{<<"auto">> => #{}}},
            {#{mode => function, function_name => <<"f">>}, #{
                <<"tool">> => #{<<"name">> => <<"f">>}
            }}
        ]
    ].

tool_result_folds_into_user_turn_test() ->
    Ok =
        <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"ok\"}]}},\"stopReason\":\"end_turn\"}">>,
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, Ok)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => ?MODEL,
        messages => [
            #{role => user, content => <<"weather?">>},
            #{
                role => assistant,
                tool_calls => [
                    #{
                        id => <<"tu_1">>,
                        name => <<"get_weather">>,
                        arguments => <<"{\"city\":\"Paris\"}">>
                    }
                ]
            },
            #{role => tool, tool_call_id => <<"tu_1">>, content => <<"sunny">>},
            #{role => tool, tool_call_id => <<"tu_2">>, content => <<"warm">>}
        ]
    }),
    [_, Assistant, ToolTurn] = maps:get(
        <<"messages">>, thinwrap_llm_fake_transport:body_json(?F:last(P))
    ),
    ?assertEqual(
        #{
            <<"role">> => <<"assistant">>,
            <<"content">> => [
                #{
                    <<"toolUse">> => #{
                        <<"toolUseId">> => <<"tu_1">>,
                        <<"name">> => <<"get_weather">>,
                        <<"input">> => #{<<"city">> => <<"Paris">>}
                    }
                }
            ]
        },
        Assistant
    ),
    ?assertEqual(
        #{
            <<"role">> => <<"user">>,
            <<"content">> => [
                #{
                    <<"toolResult">> => #{
                        <<"toolUseId">> => <<"tu_1">>,
                        <<"content">> => [#{<<"text">> => <<"sunny">>}]
                    }
                },
                #{
                    <<"toolResult">> => #{
                        <<"toolUseId">> => <<"tu_2">>,
                        <<"content">> => [#{<<"text">> => <<"warm">>}]
                    }
                }
            ]
        },
        ToolTurn
    ).

image_parts_and_empty_assistant_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => ?MODEL,
        messages => [
            #{
                role => user,
                content_parts => [
                    #{type => image, base64 => <<"AAA">>, media_type => <<"image/JPEG">>},
                    #{type => image, base64 => <<"BBB">>, media_type => <<"image/tiff">>}
                ]
            },
            #{role => assistant}
        ]
    }),
    [U, A] = maps:get(<<"messages">>, thinwrap_llm_fake_transport:body_json(?F:last(P))),
    ?assertEqual(
        [
            #{
                <<"image">> => #{
                    <<"format">> => <<"jpeg">>, <<"source">> => #{<<"bytes">> => <<"AAA">>}
                }
            },
            #{
                <<"image">> => #{
                    <<"format">> => <<"png">>, <<"source">> => #{<<"bytes">> => <<"BBB">>}
                }
            }
        ],
        maps:get(<<"content">>, U)
    ),
    ?assertEqual([#{<<"text">> => <<>>}], maps:get(<<"content">>, A)).

session_token_signing_test() ->
    {C, P} = cfg(#{session_token => <<"sess-tok-123">>}, [
        thinwrap_llm_fake_transport:resp(200, ?OK)
    ]),
    {ok, _} = thinwrap_llm:complete(C, user(<<"hi">>)),
    Req = ?F:last(P),
    ?assertEqual(
        <<"sess-tok-123">>, thinwrap_llm_fake_transport:header(Req, <<"X-Amz-Security-Token">>)
    ),
    ?assert(
        has(
            signed_headers(thinwrap_llm_fake_transport:header(Req, <<"Authorization">>)),
            <<"x-amz-security-token">>
        )
    ).

stream_fallback_deltas_test() ->
    Body =
        <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"Hello world\"},{\"toolUse\":{\"toolUseId\":\"tu_9\",\"name\":\"lookup\",\"input\":{\"q\":\"x\"}}}]}},\"stopReason\":\"tool_use\",\"usage\":{\"inputTokens\":3,\"outputTokens\":4,\"totalTokens\":7}}">>,
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
    {ok, Rev} = thinwrap_llm:stream(C, user(<<"hi">>), fun(D, A) -> [D | A] end, []),
    Deltas = lists:reverse(Rev),
    %% content delta, then one tool-call delta, then a terminal finish/usage delta.
    ?assertEqual(3, length(Deltas)),
    [D0, D1, D2] = Deltas,
    ?assertEqual(#{content_delta => <<"Hello world">>}, D0),
    #{tool_call_delta := TCD} = D1,
    ?assertMatch(#{index := 0, id := <<"tu_9">>, function_name := <<"lookup">>}, TCD),
    ?assertEqual(#{<<"q">> => <<"x">>}, json:decode(maps:get(arguments_delta, TCD))),
    ?assertMatch(#{finish_reason := tool_calls, usage := #{total_tokens := 7}, raw := #{}}, D2),
    %% The fallback still issues a single POST to /converse.
    ?assertMatch(<<_/binary>>, thinwrap_llm_fake_transport:path_of(?F:last(P))),
    ?assert(
        lists:suffix("/converse", binary_to_list(thinwrap_llm_fake_transport:path_of(?F:last(P))))
    ),
    ?assertEqual(1, length(?F:calls(P))).

stream_fallback_error_is_returned_test() ->
    {C, _} = cfg([thinwrap_llm_fake_transport:resp(403, <<"{\"Message\":\"denied\"}">>)]),
    ?assertMatch(
        {error, #{code := auth_failed, status := 403}},
        thinwrap_llm:stream(C, user(<<"hi">>), fun(D, A) -> [D | A] end, [])
    ).

error_mapping_throttling_test() ->
    {C, _} = cfg([
        thinwrap_llm_fake_transport:resp(
            429,
            <<"{\"message\":\"Too many requests, please wait before trying again.\"}">>,
            #{
                <<"x-amzn-ErrorType">> =>
                    <<"ThrottlingException:http://internal.amazon.com/coral/com.amazon.bedrock/">>
            }
        )
    ]),
    ?assertMatch(
        {error, #{
            code := rate_limited,
            status := 429,
            provider_message := <<"Too many requests, please wait before trying again.">>
        }},
        thinwrap_llm:complete(C, user(<<"x">>))
    ).

error_mapping_auth_test() ->
    {C, _} = cfg([
        thinwrap_llm_fake_transport:resp(
            403, <<"{\"Message\":\"The security token included in the request is invalid\"}">>
        )
    ]),
    ?assertMatch(
        {error, #{
            code := auth_failed,
            provider_message := <<"The security token included in the request is invalid">>
        }},
        thinwrap_llm:complete(C, user(<<"x">>))
    ).

error_mapping_context_length_test() ->
    {C, _} = cfg([
        thinwrap_llm_fake_transport:resp(
            400, <<"{\"message\":\"Input is too long for the requested model context\"}">>
        )
    ]),
    ?assertMatch(
        {error, #{code := context_length_exceeded}}, thinwrap_llm:complete(C, user(<<"x">>))
    ).

error_without_message_falls_back_test() ->
    {C, _} = cfg([thinwrap_llm_fake_transport:resp(409, <<"not json">>)]),
    ?assertMatch(
        {error, #{code := unknown, status := 409, message := <<"HTTP 409">>}},
        thinwrap_llm:complete(C, user(<<"x">>))
    ).

bad_config_type_test() ->
    ?assertError(
        {badarg, _},
        thinwrap_llm:complete(#{provider => bedrock, api_key => <<"k">>}, user(<<"x">>))
    ).

sigv4_canonical_path_is_double_encoded_for_colon_model_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, user(<<"hi">>)),
    Req = ?F:last(P),
    Wire = thinwrap_llm_fake_transport:path_of(Req),
    ?assert(has(Wire, <<"%3A">>)),
    ?assertNot(has(Wire, <<"%253A">>)),
    ?assertEqual(single(), Wire),
    ?assertNotEqual(single(), double()),
    ?assert(has(double(), <<"%253A">>)),
    Emitted = thinwrap_llm_fake_transport:header(Req, <<"Authorization">>),
    ?assertEqual(resign(Req, double(), <<"application/json">>), Emitted),
    ?assertNotEqual(resign(Req, single(), <<"application/json">>), Emitted).

sigv4_colon_free_model_unaffected_test() ->
    Plain = <<"amazon.titan-text-express-v1">>,
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => Plain, messages => [#{role => user, content => <<"hi">>}]
    }),
    Req = ?F:last(P),
    Path = <<"/model/", Plain/binary, "/converse">>,
    ?assertEqual(Path, thinwrap_llm_fake_transport:path_of(Req)),
    ?assertEqual(
        resign(Req, Path, <<"application/json">>),
        thinwrap_llm_fake_transport:header(Req, <<"Authorization">>)
    ).

bedrock_content_type_sign_equals_send_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, (user(<<"hi">>))#{
        passthrough => #{headers => #{<<"Content-Type">> => <<"application/x-amz-json-1.1">>}}
    }),
    Req = ?F:last(P),
    ?assertEqual(
        <<"application/json">>, thinwrap_llm_fake_transport:header(Req, <<"Content-Type">>)
    ),
    Auth = thinwrap_llm_fake_transport:header(Req, <<"Authorization">>),
    ?assertEqual(resign(Req, double(), <<"application/json">>), Auth),
    ?assertNotEqual(resign(Req, double(), <<"application/x-amz-json-1.1">>), Auth).

passthrough_query_is_signed_and_sent_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, (user(<<"hi">>))#{
        passthrough => #{
            query => #{<<"b">> => <<"x y">>, a => <<"1">>}, body => #{<<"extra">> => 1}
        }
    }),
    Req = ?F:last(P),
    ?assertEqual(
        <<"https://", ?HOST/binary, (single())/binary, "?a=1&b=x%20y">>, maps:get(url, Req)
    ),
    ?assertMatch(#{<<"extra">> := 1}, thinwrap_llm_fake_transport:body_json(Req)).

complete_200_without_output_object_is_empty_body_error_test_() ->
    [
        ?_test(begin
            {C, _} = cfg([thinwrap_llm_fake_transport:resp(200, B)]),
            ?assertMatch(
                {error, #{code := provider_unavailable, status := 200}},
                thinwrap_llm:complete(C, user(<<"hi">>))
            )
        end)
     || B <- [
            <<"{}">>,
            <<"{\"stopReason\":\"end_turn\",\"usage\":{\"inputTokens\":1}}">>,
            <<"{\"output\":null}">>
        ]
    ].

error_classification_test_() ->
    [
        ?_test(begin
            {C, _} = cfg([
                thinwrap_llm_fake_transport:resp(S, json:encode(#{<<"message">> => Msg}), #{
                    <<"x-amzn-ErrorType">> => Type
                })
            ]),
            ?assertMatch({error, #{code := Want}}, thinwrap_llm:complete(C, user(<<"x">>)))
        end)
     || {S, Type, Msg, Want} <- [
            {408, <<"ModelTimeoutException">>, <<"Model has timed out in processing the request.">>,
                provider_unavailable},
            {424, <<"ModelErrorException">>, <<"The model returned an error.">>,
                provider_unavailable},
            {400, <<"ValidationException">>, <<"Input is too long for requested model.">>,
                context_length_exceeded},
            {400, <<"ValidationException">>,
                <<"Value at 'requestMetadata' failed to satisfy constraint: string too long">>,
                invalid_request},
            {400, <<"ValidationException">>,
                <<"The model returned the following errors: max_tokens: 0 < 1">>, invalid_request},
            {400, <<"ThrottlingException">>, <<"Rate exceeded">>, rate_limited}
        ]
    ].

error_type_from_namespaced_body_type_test() ->
    {C, _} = cfg([
        thinwrap_llm_fake_transport:resp(
            400,
            <<"{\"__type\":\"com.amazon.coral.availability#ThrottlingException\",\"message\":\"Rate exceeded\"}">>
        )
    ]),
    ?assertMatch({error, #{code := rate_limited}}, thinwrap_llm:complete(C, user(<<"x">>))).

usage_counts_cache_read_and_write_as_input_test() ->
    Body =
        <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"x\"}]}},\"stopReason\":\"end_turn\",\"usage\":{\"inputTokens\":5,\"outputTokens\":7,\"totalTokens\":132,\"cacheReadInputTokens\":100,\"cacheWriteInputTokens\":20}}">>,
    {C, _} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
    ?assertMatch(
        {ok, #{usage := #{input_tokens := 125, output_tokens := 7, total_tokens := 132}}},
        thinwrap_llm:complete(C, user(<<"x">>))
    ).

usage_total_stays_consistent_when_vendor_total_excludes_cache_test() ->
    Body =
        <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"x\"}]}},\"stopReason\":\"end_turn\",\"usage\":{\"inputTokens\":5,\"outputTokens\":7,\"totalTokens\":12,\"cacheReadInputTokens\":100}}">>,
    {C, _} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
    ?assertMatch(
        {ok, #{usage := #{input_tokens := 105, total_tokens := 112}}},
        thinwrap_llm:complete(C, user(<<"x">>))
    ).

stop_reason_mapping_test_() ->
    [
        ?_test(begin
            Body = json:encode(#{
                <<"output">> => #{<<"message">> => #{<<"content">> => [#{<<"text">> => <<"x">>}]}},
                <<"stopReason">> => SR
            }),
            {C, _} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
            ?assertMatch({ok, #{finish_reason := Want}}, thinwrap_llm:complete(C, user(<<"x">>)))
        end)
     || {SR, Want} <- [
            {<<"model_context_window_exceeded">>, length},
            {<<"stop_sequence">>, stop},
            {<<"max_tokens">>, length},
            {<<"content_filtered">>, content_filter},
            {<<"guardrail_intervened">>, content_filter},
            {<<"something_new">>, unknown}
        ]
    ].

tool_choice_none_omits_tool_config_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, (user(<<"x">>))#{
        tools => [#{name => <<"f">>}], tool_choice => #{mode => none}
    }),
    ?assertNot(maps:is_key(<<"toolConfig">>, thinwrap_llm_fake_transport:body_json(?F:last(P)))).

region_must_be_a_region_name_test_() ->
    [
        ?_test(begin
            {C, P} = cfg(#{region => R}, [thinwrap_llm_fake_transport:resp(200, ?OK)]),
            ?assertMatch(
                {error, #{code := invalid_request}}, thinwrap_llm:complete(C, user(<<"x">>))
            ),
            ?assertEqual([], ?F:calls(P))
        end)
     || R <- [
            <<"us-east-1.evil.example/x">>,
            <<"us-east-1@evil.example">>,
            <<"US-EAST-1">>,
            <<>>,
            <<"us east 1">>,
            <<"us-east-1\n">>
        ]
    ].

base_url_without_host_is_invalid_request_test() ->
    {C, _} = cfg(#{base_url => <<"not-a-url">>}, [thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {error, E} = thinwrap_llm:complete(C, user(<<"x">>)),
    ?assertMatch(#{code := invalid_request}, E),
    ?assertEqual(
        <<"Bedrock: invalid `base_url`/`region` \x{2014} could not derive a host from 'not-a-url'"/utf8>>,
        maps:get(message, E)
    ).

default_https_port_is_not_signed_into_host_test() ->
    {C, P} = cfg(#{base_url => <<"https://", ?HOST/binary, ":443">>}, [
        thinwrap_llm_fake_transport:resp(200, ?OK)
    ]),
    {ok, _} = thinwrap_llm:complete(C, user(<<"hi">>)),
    Req = ?F:last(P),
    ?assertEqual(
        resign(Req, double(), <<"application/json">>),
        thinwrap_llm_fake_transport:header(Req, <<"Authorization">>)
    ).

headers_merge_case_insensitively_before_signing_test() ->
    {C, P} = cfg(
        #{headers => #{<<"x-trace">> => <<"cfg">>, <<"content-type">> => <<"text/plain">>}}, [
            thinwrap_llm_fake_transport:resp(200, ?OK)
        ]
    ),
    {ok, _} = thinwrap_llm:complete(C, (user(<<"hi">>))#{
        passthrough => #{headers => #{<<"X-Trace">> => <<"req">>}}
    }),
    #{headers := H} = ?F:last(P),
    Names = [string:lowercase(K) || K <- maps:keys(H)],
    ?assertEqual(1, length([N || N <- Names, N =:= <<"x-trace">>])),
    ?assertEqual(1, length([N || N <- Names, N =:= <<"content-type">>])),
    ?assertEqual(<<"req">>, maps:get(<<"X-Trace">>, H)),
    ?assertEqual(<<"application/json">>, maps:get(<<"Content-Type">>, H)).

status_402_is_rate_limited_and_413_invalid_request_test_() ->
    [
        ?_test(begin
            {C, _} = cfg([thinwrap_llm_fake_transport:resp(S, <<"{\"message\":\"nope\"}">>)]),
            ?assertMatch({error, #{code := Want}}, thinwrap_llm:complete(C, user(<<"x">>)))
        end)
     || {S, Want} <- [{402, rate_limited}, {413, invalid_request}]
    ].

total_tokens_is_input_plus_output_test() ->
    Body =
        <<"{\"output\":{\"message\":{\"content\":[{\"text\":\"x\"}]}},\"stopReason\":\"end_turn\",\"usage\":{\"inputTokens\":5,\"outputTokens\":7,\"totalTokens\":999,\"cacheWriteInputTokens\":20}}">>,
    {C, _} = cfg([thinwrap_llm_fake_transport:resp(200, Body)]),
    ?assertMatch(
        {ok, #{usage := #{input_tokens := 25, total_tokens := 32}}},
        thinwrap_llm:complete(C, user(<<"x">>))
    ).

base_url_path_prefix_is_signed_test() ->
    {C, P} = cfg(#{base_url => <<"https://", ?HOST/binary, "/gateway/bedrock">>}, [
        thinwrap_llm_fake_transport:resp(200, ?OK)
    ]),
    {ok, _} = thinwrap_llm:complete(C, user(<<"hi">>)),
    Req = ?F:last(P),
    ?assertEqual(
        <<"/gateway/bedrock", (single())/binary>>, thinwrap_llm_fake_transport:path_of(Req)
    ),
    Auth = thinwrap_llm_fake_transport:header(Req, <<"Authorization">>),
    ?assertEqual(
        resign(Req, <<"/gateway/bedrock", (double())/binary>>, <<"application/json">>), Auth
    ),
    ?assertNotEqual(resign(Req, double(), <<"application/json">>), Auth).

empty_text_parts_are_dropped_test() ->
    {C, P} = cfg([thinwrap_llm_fake_transport:resp(200, ?OK)]),
    {ok, _} = thinwrap_llm:complete(C, #{
        model => ?MODEL,
        messages => [
            #{
                role => user,
                content_parts => [#{type => text, text => <<>>}, #{type => text, text => <<"hi">>}]
            }
        ]
    }),
    [M] = maps:get(<<"messages">>, thinwrap_llm_fake_transport:body_json(?F:last(P))),
    ?assertEqual([#{<<"text">> => <<"hi">>}], maps:get(<<"content">>, M)).

context_overflow_phrases_test_() ->
    [
        ?_test(begin
            {C, _} = cfg([
                thinwrap_llm_fake_transport:resp(400, json:encode(#{<<"message">> => Msg}), #{
                    <<"x-amzn-ErrorType">> => <<"ValidationException">>
                })
            ]),
            ?assertMatch({error, #{code := Want}}, thinwrap_llm:complete(C, user(<<"x">>)))
        end)
     || {Msg, Want} <- [
            {<<"The model returned the following errors: prompt is too long: 205000 tokens > 200000 maximum">>,
                context_length_exceeded},
            {<<"This model's maximum context length is 128000 tokens.">>, context_length_exceeded},
            {<<"Too many tokens per day, please wait before trying again.">>, invalid_request},
            {<<"context window settings are invalid">>, invalid_request}
        ]
    ].
