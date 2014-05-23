#!/usr/bin/env escript
%% -*- erlang -*-

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
%
%
filename() -> test_util:build_file("t/temp.050").
rows() -> 250.

main(_) ->
    test_util:init_code_path(),
    etap:plan(10),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

%% @todo Determine if this number should be greater to see if the btree was
%% broken into multiple nodes. AKA "How do we appropiately detect if multiple
%% nodes were created."
test()->
    Sorted = [{Seq, random:uniform()} || Seq <- lists:seq(1, rows())],
    etap:ok(test_kvs(Sorted), "Testing sorted keys"),
    ok.

test_kvs(KeyValues) ->
    ReduceFun = fun
        (reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,

    Keys = [K || {K, _} <- KeyValues],

    {ok, Db} = cbt:open(filename(), [{test, [{compression, none},
                                             {reduce, ReduceFun}]}],
                        [create,overwrite]),


    etap:ok(is_pid(Db), "Created DB"),
    etap:is(0, get_size(Db, test), "Empty btrees have a 0 size."),

    {ok, _, EmptyRes} = cbt:transact(Db, fun() ->
                    cbt:fold(test, fun(_, X) -> {ok, X+1} end, 0)
            end),

    etap:is(EmptyRes, 0, "Folding over an empty btree"),

    ok = cbt:transact(Db, fun() ->
                    cbt:add_remove(test, KeyValues, [])
            end),

    etap:is((get_size(Db, test) > 0), true, "Non empty btrees have a size > 0."),

    [LastKey | _] = Keys,

    {ok, Old, New, Stored} = cbt:transact(Db, fun() ->
                    [{ok, Old1}] = cbt:lookup(test, [LastKey]),

                    New1 = {LastKey, 10},
                    ok = cbt:add_remove(test, [New1], []),
                    [{ok, Stored1}] = cbt:lookup(test, [LastKey]),
                    {ok, Old1, New1, Stored1}
            end),
    [LastKV | _] = KeyValues,

    etap:is(Old, LastKV, "lookup ok"),
    etap:is(Old =/= New, true, "value updated"),
    etap:is(New =:= Stored, true, "value stored ok"),
    etap:is(Stored, {LastKey, 10}, "last {K,V} stored ok"),

    ok = cbt:transact(Db, fun() ->
                    cbt:add_remove(test, [], Keys)
            end),

    etap:is(0, get_size(Db, test),
            "After removing all keys btree size is 0."),


    %% Third chunk (close out)
    etap:is(ok, cbt:close(Db), "closing out"),

    timer:sleep(300),
    true.

get_size(Db, BtName) ->
    cbt:transact(Db, fun() ->
                    cbt:size(BtName)
            end).
