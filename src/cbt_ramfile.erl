-module(cbt_ramfile).
-behaviour(gen_server).
-behaviour(cbt_backend).


-export([append_term/2, append_term/3,
         append_term_crc32/2,
         pread_term/2,
         sync/1,
         truncate/2,
         bytes/1,
         empty/1]).

-export([open/1, open/2,
         close/1,
         read_header/1,
         write_header/2,
         append_binary/2,
         append_binary_crc32/2,
         append_raw_chunk/2,
         pread_binary/2,
         pread_iolist/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


-define(SIZE_BLOCK, 16#1000). % 4 KiB

-include("cbt.hrl").


append_term(Pid, Term) -> append_term(Pid, Term, []).

append_term(Pid, Term, Options) ->
  Comp = cbt_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
  append_binary(Pid, cbt_compress:compress(Term, Comp)).

append_term_crc32(Fd, Term) ->
  append_term_crc32(Fd, Term, []).

append_term_crc32(Fd, Term, Options) ->
  Comp = cbt_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
  append_binary_crc32(Fd, cbt_compress:compress(Term, Comp)).


pread_term(Pid, Pos) ->
  {ok, Bin} = pread_binary(Pid, Pos),
  {ok, cbt_compress:decompress(Bin)}.


sync(_Pid) -> ok.

read_header(Pid) ->
  case find_header(Pid) of
    {ok, Bin, Pos} ->
      {ok, binary_to_term(Bin), Pos};
    Else ->
      Else
  end.

write_header(Pid, Data) ->
  Bin = term_to_binary(Data),
  Crc32 = erlang:crc32(Bin),
  % now we assemble the final header binary and write to disk
  FinalBin = <<Crc32:32/integer, Bin/binary>>,
  write_header_bin(Pid, FinalBin).


open(Name) -> open(Name, []).

open(_Name, _Options) ->
  gen_server:start_link(?MODULE, [], []).

close(Pid) -> gen_server:call(Pid, close).

append_binary(Pid, Bin) ->
  gen_server:call(Pid, {append_bin, assemble_file_chunk(Bin)}).

append_binary_crc32(Fd, Bin) ->
  gen_server:call(
    Fd,
    {append_bin, assemble_file_chunk(Bin, erlang:crc32(Bin))}, infinity).

append_raw_chunk(Fd, Chunk) ->
  gen_server:call(Fd, {append_bin, Chunk}, infinity).



pread_binary(Pid, Pos) ->
  case pread_iolist(Pid, Pos) of
    {ok, IoList} ->
      {ok, iolist_to_binary(IoList)};
    Error ->
      Error
  end.

pread_iolist(Pid, Pos) ->
  Res = gen_server:call(Pid, {pread_iolist, Pos}),
  case Res of
    {ok, IoList, <<>>} -> {ok, IoList};
    {ok, IoList, << Crc32:32/integer >>} ->
      case erlang:crc32(IoList) of
        Crc32 ->
          {ok, IoList};
        _ ->
          error_logger:info_msg("File corruption in ~p at position ~B",
                                [Pid, Pos]),
          exit({file_corruption, <<"file corruption">>})
      end;
    Error ->
      Error
  end.

write_header_bin(Pid, Bin) ->
  gen_server:call(Pid, {write_header, Bin}, infinity).

find_header(Pid) ->
  gen_server:call(Pid, find_header, infinity).

bytes(Pid) ->
  gen_server:call(Pid, bytes).

empty(Pid) ->
  truncate(Pid, 0).

truncate(Pid, Pos) ->
  gen_server:call(Pid, {truncate, Pos}).

assemble_file_chunk(Bin) ->
  [<<0:1/integer, (iolist_size(Bin)):31/integer>>, Bin].

assemble_file_chunk(Bin, Crc32) ->
  [<<1:1/integer, (iolist_size(Bin)):31/integer, Crc32:32/integer>>, Bin].



init([]) ->
  {ok, Fd} = file:open("", [ram, read, write, binary]),
  {ok, #{ fd => Fd, eof => 0}}.


handle_call({append_bin, Bin}, _From, #{ fd := Fd, eof := Pos } = State) ->
  Blocks = make_blocks(Pos rem ?SIZE_BLOCK, Bin),
  Size = iolist_size(Blocks),
  {Reply, NewState} = case file:pwrite(Fd, Pos, Blocks) of
                         ok ->
                           Eof = Pos + Size,
                           {{ok, Pos, Size}, State#{ eof => Eof }};
                         Error ->
                           {Error, State}
                       end,
  {reply, Reply, NewState};
handle_call({pread_iolist, Pos}, _From, #{ fd := Fd } =  State) ->
  {RawData, NextPos} = try
                         read_raw_iolist_int(
                           Fd, Pos, 2 * ?SIZE_BLOCK - (Pos rem ?SIZE_BLOCK)
                          )
                       catch
                         _:_ ->
                           read_raw_iolist_int(Fd, Pos, 4)
                       end,
  {Begin, RestRawData} = split_iolist(RawData, 4, []),
  <<Prefix:1/integer, Len:31/integer>> = iolist_to_binary(Begin),
  Reply = case Prefix of
            1 ->
              {Crc32, IoList} = extract_crc32(
                                  maybe_read_more_iolist(RestRawData,
                                                         4 + Len, NextPos,
                                                         Fd)
                                 ),
              {ok, IoList, Crc32};
            0 ->
              IoList = maybe_read_more_iolist(RestRawData, Len, NextPos, Fd),
              {ok, IoList, <<>>}
          end,
  {reply, Reply, State};
handle_call({write_header, Bin}, _From, #{ fd := Fd, eof := Pos } = State) ->
  BinSize = byte_size(Bin),
  {Padding, Pos2} = case (Pos rem ?SIZE_BLOCK) of
                      0 -> { <<>>, Pos };
                      BlockOffset ->
                        Delta = (?SIZE_BLOCK - BlockOffset),
                        Pos1 = Pos + Delta,
                        { << 0:(8 * Delta) >>, Pos1 }
                    end,
  IoList = [Padding, << 1, BinSize:32/integer >> | make_blocks(5, [Bin])],
  {Reply, NewState} = case file:pwrite(Fd, Pos, IoList) of
                        ok ->
                           {{ok, Pos2}, State#{ eof => Pos + iolist_size(IoList) }};
                         Error ->
                           {Error, State}
                       end,
  {reply, Reply,NewState};
handle_call(find_header, _From, #{ fd := Fd, eof := Pos } = State) ->
  Reply = find_header(Fd, Pos div ?SIZE_BLOCK),
  {reply, Reply, State};
handle_call(bytes, _From, #{ fd := Fd } = State) ->
  {reply, file:position(Fd, eof), State};

handle_call({truncate, Pos}, _From, #{ fd := Fd } = State) ->
  {ok, Pos} = file:position(Fd, Pos),
  {Reply, NewState} = case file:truncate(Fd) of
                         ok ->
                           {ok, State#{ eof => Pos }};
                         Error ->
                           {Error, State}
                       end,
  {reply, Reply, NewState};
handle_call(close, _From, State) ->
  {stop, normal, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.

terminate(_Reason, #{ fd := Fd }) ->
  file:close(Fd).



find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, Bin} ->
        {ok, Bin, Block * ?SIZE_BLOCK};
    _Error ->
        find_header(Fd, Block -1)
    end.

load_header(Fd, Block) ->
    {ok, <<1, HeaderLen:32/integer, RestBlock/binary>>} =
        file:pread(Fd, Block * ?SIZE_BLOCK, ?SIZE_BLOCK),
    TotalBytes = calculate_total_read_len(5, HeaderLen),
    RawBin = case TotalBytes > byte_size(RestBlock) of
        false ->
            <<RawBin1:TotalBytes/binary, _/binary>> = RestBlock,
            RawBin1;
        true ->
            {ok, Missing} = file:pread(
                    Fd, (Block * ?SIZE_BLOCK) + 5 + byte_size(RestBlock),
                    TotalBytes - byte_size(RestBlock)),
            <<RestBlock/binary, Missing/binary>>
    end,
    <<Crc32Sig:32/integer, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(RawBin, 5)),
    Crc32Sig = erlang:crc32(HeaderBin),
    {ok, HeaderBin}.


maybe_read_more_iolist(Buffer, DataSize, NextPos, Fd) ->
    case iolist_size(Buffer) of
        BufferSize when DataSize =< BufferSize ->
            {Buffer2, _} = split_iolist(Buffer, DataSize, []),
            Buffer2;
        BufferSize ->
            {Missing, _} = read_raw_iolist_int(Fd, NextPos, DataSize-BufferSize),
            [Buffer, Missing]
    end.

read_raw_iolist_int(Fd, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
    case file:pread(Fd, Pos, TotalBytes) of
        {ok, <<RawBin:TotalBytes/binary>>} ->
            {remove_block_prefixes(RawBin, BlockOffset), Pos + TotalBytes};
        {ok, RawBin} ->
            UnexpectedBin = {
                    unexpected_binary,
                    {at, Pos},
                    {wanted_bytes, TotalBytes},
                    {got, byte_size(RawBin), RawBin}
                    },
            throw({read_error, UnexpectedBin});
        Else ->
            throw({read_error, Else})
    end.

extract_crc32(FullIoList) ->
  {CrcList, IoList} = split_iolist(FullIoList, 4, []),
  {iolist_to_binary(CrcList), IoList}.

calculate_total_read_len(0, FinalLen) ->
  calculate_total_read_len(1, FinalLen) + 1;
calculate_total_read_len(BlockOffset, FinalLen) ->
  case ?SIZE_BLOCK - BlockOffset of
    BlockLeft when BlockLeft >= FinalLen ->
      FinalLen;
    BlockLeft ->
      FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK -1)) +
      if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK -1)) =:= 0 -> 0;
         true -> 1 end
  end.



remove_block_prefixes(<<>>, _BlockOffset) ->
  [];
remove_block_prefixes(<<_BlockPrefix, Rest/binary>>, 0) ->
  remove_block_prefixes(Rest, 1);
remove_block_prefixes(Bin, BlockOffset) ->
  BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
  case size(Bin) of
    Size when Size > BlockBytesAvailable ->
      <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
      [DataBlock | remove_block_prefixes(Rest, 0)];
    _Size ->
      [Bin]
  end.

make_blocks(_BlockOffset, []) ->
  [];
make_blocks(0, IoList) ->
  [<<0>> | make_blocks(1, IoList)];
make_blocks(BlockOffset, IoList) ->
  case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
    {Begin, End} ->
      [Begin | make_blocks(0, End)];
    _SplitRemaining ->
      IoList
  end.

%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.

split_iolist(List, 0, BeginAcc) ->
  {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
  SplitAt;
split_iolist([Bin | Rest], SplitAt, BeginAcc) when is_binary(Bin), SplitAt > byte_size(Bin) ->
  split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([Bin | Rest], SplitAt, BeginAcc) when is_binary(Bin) ->
  <<Begin:SplitAt/binary,End/binary>> = Bin,
  split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
  case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
      {Begin, [End | Rest]};
    SplitRemaining ->
      split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
  end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
  split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).
