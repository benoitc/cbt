% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(cbt_compress).

-export([compress/2, decompress/1, is_compressed/2]).

-include("cbt.hrl").

% binaries compressed with snappy have their first byte set to this value
-define(SNAPPY_PREFIX, 1).
% binaries compressed with gzip have their first byte set to this value
-define(GZIP_PREFIX, 2).
% binaries compressed with lz4 have their first byte set to this value
-define(LZ4_PREFIX, 3).
% Term prefixes documented at:
%      http://www.erlang.org/doc/apps/erts/erl_ext_dist.html
-define(TERM_PREFIX, 131).
-define(COMPRESSED_TERM_PREFIX, 131, 80).

-type compression_method() :: snappy | lz4 | gzip
                              | {deflate, Level::integer()} | none.
-export_type([compression_method/0]).


use_compressed(UncompressedSize, CompressedSize)
        when CompressedSize < UncompressedSize ->
    true;
use_compressed(_UncompressedSize, _CompressedSize) ->
    false.


%% @doc compress an encoded binary with the following type. When an
%% erlang term is given it is encoded to a binary.
-spec compress(Bin::binary()|term(), Method::compression_method()) -> Bin::binary().
compress(<<?SNAPPY_PREFIX, _/binary>> = Bin, snappy) ->
    Bin;
compress(<<?GZIP_PREFIX, _/binary>> = Bin, gzip) ->
    Bin;
compress(<<?LZ4_PREFIX, _/binary>> = Bin, lz4) ->
    Bin;
compress(<<?SNAPPY_PREFIX, _/binary>> = Bin, Method) ->
    compress(decompress(Bin), Method);
compress(<<?GZIP_PREFIX, _/binary>> = Bin, Method) ->
    compress(decompress(Bin), Method);
compress(<<?LZ4_PREFIX, _/binary>> = Bin, Method) ->
    compress(decompress(Bin), Method);
compress(<<?TERM_PREFIX, _/binary>> = Bin, Method) ->
    compress(decompress(Bin), Method);
compress(Term, {deflate, Level}) ->
    term_to_binary(Term, [{minor_version, 1}, {compressed, Level}]);
compress(Term, snappy) ->
    Bin = ?term_to_bin(Term),
    {ok, CompressedBin} = snappy:compress(Bin),
    case use_compressed(byte_size(Bin), byte_size(CompressedBin)) of
        true ->
            <<?SNAPPY_PREFIX, CompressedBin/binary>>;
        false ->
            Bin
    end;
compress(Term, gzip) ->
    Bin = ?term_to_bin(Term),
    {ok, CompressedBin} = zlib:gzip(Bin),
    case use_compressed(byte_size(Bin), byte_size(CompressedBin)) of
        true ->
            <<?GZIP_PREFIX, CompressedBin/binary>>;
        false ->
            Bin
    end;
compress(Term, lz4) ->
    Bin = ?term_to_bin(Term),
    {ok, CompressedBin} = lz4:compress(erlang:iolist_to_binary(Bin)),
    case use_compressed(byte_size(Bin), byte_size(CompressedBin)) of
        true ->
            <<?LZ4_PREFIX, CompressedBin>>;
        false ->
           Bin
    end;
compress(Term, none) ->
    ?term_to_bin(Term).

%% @doc decompress a binary to an erlang decoded term.
-spec decompress(Bin::binary()) -> Term::term().
decompress(<<?SNAPPY_PREFIX, Rest/binary>>) ->
    {ok, TermBin} = snappy:decompress(Rest),
    binary_to_term(TermBin);
decompress(<<?GZIP_PREFIX, Rest/binary>>) ->
    {ok, TermBin} = zlip:gunzip(Rest),
    binary_to_term(TermBin);
decompress(<<?LZ4_PREFIX, Rest/binary>>) ->
    {ok, TermBin} = lz4:decompress(Rest),
    binary_to_term(TermBin);
decompress(<<?TERM_PREFIX, _/binary>> = Bin) ->
    binary_to_term(Bin).

%% @doc check if the binary has been compressed.
-spec is_compressed(Bin::binary()|term(),
                    Method::compression_method()) -> true | false.
is_compressed(<<?SNAPPY_PREFIX, _/binary>>, snappy) ->
    true;
is_compressed(<<?GZIP_PREFIX, _/binary>>, gzip) ->
    true;
is_compressed(<<?LZ4_PREFIX, _/binary>>, lz4) ->
    true;
is_compressed(<<?COMPRESSED_TERM_PREFIX, _/binary>>, {deflate, _Level}) ->
    true;
is_compressed(<<?COMPRESSED_TERM_PREFIX, _/binary>>, _Method) ->
    false;
is_compressed(<<?TERM_PREFIX, _/binary>>, none) ->
    true;
is_compressed(_Term, _Method) ->
    false.
