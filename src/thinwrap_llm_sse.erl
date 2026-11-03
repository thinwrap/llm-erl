-module(thinwrap_llm_sse).
-moduledoc false.

%% Incremental Server-Sent-Events parsing. Body chunks arrive split anywhere —
%% mid-line, mid-event, between the CR and LF of one line break — so the
%% parser keeps the unterminated tail between calls. It yields the data
%% payload of each event, skips comments and other fields, joins multi-line
%% data with "\n", and stops at the `[DONE]` sentinel.

-export([new/0, new/1, feed/2, finish/1, done/1]).

-export_type([state/0]).

%% A single line, or the data of one event, beyond this is refused: an
%% adversarial upstream could otherwise stream without a line or event
%% boundary and grow the buffer without limit (the siblings' 16 MiB cap).
-define(MAX, 16 * 1024 * 1024).

-opaque state() :: #{
    buf := binary(),
    cr := boolean(),
    data := [binary()],
    size := non_neg_integer(),
    done := boolean(),
    max := pos_integer()
}.

-doc false.
-spec new() -> state().
new() -> new(?MAX).

-doc false.
%% A parser with a custom cap (tests).
-spec new(pos_integer()) -> state().
new(Max) ->
    #{buf => <<>>, cr => false, data => [], size => 0, done => false, max => Max}.

-doc false.
%% Whether the `[DONE]` sentinel arrived.
-spec done(state()) -> boolean().
done(#{done := D}) -> D.

-doc false.
%% Consume one body chunk; returns the complete payloads in order.
-spec feed(binary(), state()) -> {[binary()], state()}.
feed(_Chunk, #{done := true} = S) ->
    {[], S};
feed(Chunk0, #{buf := Buf, cr := Cr} = S) ->
    Chunk =
        case {Cr, Chunk0} of
            %% The previous chunk ended on CR; this LF completes that CRLF.
            {true, <<"\n", Rest/binary>>} -> Rest;
            _ -> Chunk0
        end,
    lines(<<Buf/binary, Chunk/binary>>, S#{cr := false}, []).

-doc false.
%% The body ended: the unterminated tail is a final line, and a pending event
%% is dispatched even without its blank line.
-spec finish(state()) -> {[binary()], state()}.
finish(#{done := true} = S) ->
    {[], S};
finish(#{buf := Buf} = S0) ->
    {Out, S1} =
        case Buf of
            <<>> -> {[], S0};
            _ -> line(Buf, S0#{buf := <<>>}, [])
        end,
    case S1 of
        #{done := true} ->
            {lists:reverse(Out), S1};
        #{data := []} ->
            {lists:reverse(Out), S1};
        #{data := Parts} ->
            case join(Parts) of
                <<"[DONE]">> -> {lists:reverse(Out), S1#{data := [], size := 0, done := true}};
                P -> {lists:reverse([P | Out]), S1#{data := [], size := 0}}
            end
    end.

lines(Data, #{max := Max} = S, Acc) ->
    case binary:match(Data, [<<"\r\n">>, <<"\r">>, <<"\n">>]) of
        nomatch when byte_size(Data) > Max ->
            cap_error(<<"SSE line exceeds the 16 MiB cap">>);
        nomatch ->
            {lists:reverse(Acc), S#{buf := Data}};
        {Pos, Len} ->
            Line = binary:part(Data, 0, Pos),
            Rest = binary:part(Data, Pos + Len, byte_size(Data) - Pos - Len),
            %% A CR as the very last byte may be the first half of a CRLF whose
            %% LF arrives in the next chunk.
            Cr = Len =:= 1 andalso Rest =:= <<>> andalso binary:last(Data) =:= $\r,
            case line(Line, S, Acc) of
                {Acc1, #{done := true} = S1} -> {lists:reverse(Acc1), S1#{buf := <<>>}};
                {Acc1, S1} -> lines(Rest, S1#{cr := Cr}, Acc1)
            end
    end.

line(<<>>, #{data := []} = S, Acc) ->
    {Acc, S};
line(<<>>, #{data := Parts} = S, Acc) ->
    case join(Parts) of
        <<"[DONE]">> -> {Acc, S#{data := [], size := 0, done := true}};
        P -> {[P | Acc], S#{data := [], size := 0}}
    end;
line(<<"data:", Seg0/binary>>, #{data := Parts, size := Size, max := Max} = S, Acc) ->
    %% Per the SSE spec only ONE optional leading space is stripped; further
    %% leading whitespace is payload.
    Seg =
        case Seg0 of
            <<" ", R/binary>> -> R;
            _ -> Seg0
        end,
    Size1 = Size + byte_size(Seg) + 1,
    case Size1 > Max of
        true -> cap_error(<<"SSE event exceeds the 16 MiB cap">>);
        false -> {Acc, S#{data := [Seg | Parts], size := Size1}}
    end;
line(_Other, S, Acc) ->
    %% Comments (":" prefix) and the other fields (event:, id:, retry:).
    {Acc, S}.

join(RevParts) -> iolist_to_binary(lists:join(<<"\n">>, lists:reverse(RevParts))).

-spec cap_error(binary()) -> no_return().
cap_error(Msg) ->
    thinwrap_llm_error:fail(
        thinwrap_llm_error:new(provider_unavailable, #{message => Msg, provider_message => Msg})
    ).
