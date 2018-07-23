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

-module(cbt_ramfile_tests).

-include("cbt_tests.hrl").

-define(BLOCK_SIZE, 4096).
-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(foreach(Fs), {foreach, fun setup/0, fun teardown/1, Fs}).


setup() ->
    {ok, Fd} = cbt_ramfile:open(?tempfile()),
    Fd.

teardown(Fd) ->
    ok = cbt_ramfile:close(Fd).


open_close_test_() ->
    {
        "Test for proper file open and close",
        [
            ?setup(fun should_return_pid_on_file_open/1),
            should_close_file_properly(),
            ?setup(fun should_create_empty_new_files/1)
        ]
    }.


should_return_pid_on_file_open(Fd) ->
    ?_assert(is_pid(Fd)).

should_close_file_properly() ->
    {ok, Fd} = cbt_ramfile:open(?tempfile()),
    ok = cbt_ramfile:close(Fd),
    ?_assert(true).

should_create_empty_new_files(Fd) ->
    ?_assertMatch({ok, 0}, cbt_ramfile:bytes(Fd)).


read_write_test_() ->
    {
        "Common file read/write tests",
        ?foreach([
            fun should_increase_file_size_on_write/1,
            fun should_write_and_read_term/1,
            fun should_write_and_read_binary/1,
            fun should_write_and_read_large_binary/1,
            fun should_read_iolist/1,
            fun should_fsync/1,
            fun should_not_read_beyond_eof/1,
            fun should_truncate/1
        ])
    }.


should_increase_file_size_on_write(Fd) ->
    {ok, 0, _} = cbt_ramfile:append_term(Fd, foo),
    {ok, Size} = cbt_ramfile:bytes(Fd),
    ?_assert(Size > 0).

should_return_current_file_size_on_write(Fd) ->
    {ok, 0, _} = cbt_ramfile:append_term(Fd, foo),
    {ok, Size} = cbt_ramfile:bytes(Fd),
    ?_assertMatch({ok, Size, _}, cbt_ramfile:append_term(Fd, bar)).

should_write_and_read_term(Fd) ->
    {ok, Pos, _} = cbt_ramfile:append_term(Fd, foo),
    ?_assertMatch({ok, foo}, cbt_ramfile:pread_term(Fd, Pos)).

should_write_and_read_binary(Fd) ->
    {ok, Pos, _} = cbt_ramfile:append_binary(Fd, <<"fancy!">>),
    ?_assertMatch({ok, <<"fancy!">>}, cbt_ramfile:pread_binary(Fd, Pos)).

should_write_and_read_large_binary(Fd) ->
    BigBin = list_to_binary(lists:duplicate(100000, 0)),
    {ok, Pos, _} = cbt_ramfile:append_binary(Fd, BigBin),
    ?_assertMatch({ok, BigBin}, cbt_ramfile:pread_binary(Fd, Pos)).

should_read_iolist(Fd) ->
    %% append_binary == append_iolist?
    %% Possible bug in pread_iolist or iolist() -> append_binary
    {ok, Pos, _} = cbt_ramfile:append_binary(Fd, ["foo", $m, <<"bam">>]),
    {ok, IoList} = cbt_ramfile:pread_iolist(Fd, Pos),
    ?_assertMatch(<<"foombam">>, iolist_to_binary(IoList)).

should_fsync(Fd) ->
    {"How does on test fsync?", ?_assertMatch(ok, cbt_ramfile:sync(Fd))}.

should_not_read_beyond_eof(_) ->
    {"No idea how to test reading beyond EOF", ?_assert(true)}.

should_truncate(Fd) ->
    {ok, 0, _} = cbt_ramfile:append_term(Fd, foo),
    {ok, Size} = cbt_ramfile:bytes(Fd),
    BigBin = list_to_binary(lists:duplicate(100000, 0)),
    {ok, _, _} = cbt_ramfile:append_binary(Fd, BigBin),
    ok = cbt_ramfile:truncate(Fd, Size),
    ?_assertMatch({ok, foo}, cbt_ramfile:pread_term(Fd, 0)).


header_test_() ->
    {
        "File header read/write tests",
        [
            ?foreach([
                fun should_write_and_read_atom_header/1,
                fun should_write_and_read_tuple_header/1,
                fun should_write_and_read_second_header/1,
                fun should_truncate_second_header/1,
                fun should_produce_same_file_size_on_rewrite/1,
                fun should_save_headers_larger_than_block_size/1
            ])
        ]
    }.


should_write_and_read_atom_header(Fd) ->
    {ok, HeaderPos} = cbt_ramfile:write_header(Fd, hello),
    ?_assertMatch({ok, hello, HeaderPos}, cbt_ramfile:read_header(Fd)).

should_write_and_read_tuple_header(Fd) ->
    {ok, _} = cbt_ramfile:write_header(Fd, {<<"some_data">>, 32}),
    ?_assertMatch({ok, {<<"some_data">>, 32}, _}, cbt_ramfile:read_header(Fd)).

should_write_and_read_second_header(Fd) ->
    {ok, 0} = cbt_ramfile:write_header(Fd, {<<"some_data">>, 32}),
    {ok, 4096} = cbt_ramfile:write_header(Fd, [foo, <<"more">>]),
    ?_assertMatch({ok, [foo, <<"more">>], 4096}, cbt_ramfile:read_header(Fd)).

should_truncate_second_header(Fd) ->
    {ok, _} = cbt_ramfile:write_header(Fd, {<<"some_data">>, 32}),
    {ok, Size} = cbt_ramfile:bytes(Fd),
    {ok, _} = cbt_ramfile:write_header(Fd, [foo, <<"more">>]),
    ok = cbt_ramfile:truncate(Fd, Size),
    ?_assertMatch({ok, {<<"some_data">>, 32}, 0}, cbt_ramfile:read_header(Fd)).

should_produce_same_file_size_on_rewrite(Fd) ->
    {ok, _} = cbt_ramfile:write_header(Fd, {<<"some_data">>, 32}),
    {ok, Size1} = cbt_ramfile:bytes(Fd),
    {ok, _} = cbt_ramfile:write_header(Fd, [foo, <<"more">>]),
    {ok, Size2} = cbt_ramfile:bytes(Fd),
    ok = cbt_ramfile:truncate(Fd, Size1),
    {ok, _} = cbt_ramfile:write_header(Fd, [foo, <<"more">>]),
    ?_assertMatch({ok, Size2}, cbt_ramfile:bytes(Fd)).

should_save_headers_larger_than_block_size(Fd) ->
    Header = erlang:make_tuple(5000, <<"CouchDB">>),
    {ok, _} = cbt_ramfile:write_header(Fd, Header),
    {"COUCHDB-1319", ?_assertMatch({ok, Header, 0},
                                   cbt_ramfile:read_header(Fd))}.








write_random_data(Fd) ->
    write_random_data(Fd, 100 + rand:uniform(1000)).

write_random_data(Fd, 0) ->
    {ok, Bytes} = cbt_ramfile:bytes(Fd),
    {ok, (1 + Bytes div ?BLOCK_SIZE) * ?BLOCK_SIZE};
write_random_data(Fd, N) ->
    Choices = [foo, bar, <<"bizzingle">>, "bank", ["rough", stuff]],
    Term = lists:nth(rand:uniform(4) + 1, Choices),
    {ok, _, _} = cbt_ramfile:append_term(Fd, Term),
    write_random_data(Fd, N - 1).
