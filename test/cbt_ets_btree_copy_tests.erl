-module(cbt_ets_btree_copy_tests).

-include_lib("include/cbt.hrl").
-include("cbt_tests.hrl").


setup_copy(_) ->
    ReduceFun = fun(reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,

    OriginalFileName = ?tempfile(),
    CopyEts = test_db,
    {ok, Fd} = cbt_file:open(OriginalFileName, [create, overwrite]),
    cbt_ets:new(CopyEts),
    {ReduceFun, OriginalFileName, CopyEts, Fd}.

teardown(_, {_, OriginalFileName, CopyEts, Fd}) ->
    ok = cbt_file:close(Fd),
    ok = cbt_ets:delete(CopyEts),
    ok = file:delete(OriginalFileName).

btree_copy_test_() ->
    TNumItems = [50, 100, 300, 700, 811, 2333, 6594, 9999, 15003, 21477,
                 38888, 66069, 150123, 420789, 711321],
    {
        "Copy BTree",
        {
            foreachx,
            fun setup_copy/1, fun teardown/2,
            [{N, fun  should_copy_btree/2} || N <- TNumItems]
        }
    }.

btree_copy_compressed_test_() ->
    TNumItems = [50, 100, 300, 700, 811, 2333, 6594, 9999, 15003, 21477,
                 38888, 66069, 150123, 420789, 711321],
    {
        "Copy Compressed BTree",
        {
            foreachx,
            fun setup_copy/1, fun teardown/2,
            [{N, fun  should_copy_compressed_btree/2} || N <- TNumItems]
        }
    }.

should_copy_btree(NumItems, {ReduceFun, _OriginalFileName, CopyEts, Fd}) ->
    KVs = [{I, I} || I <- lists:seq(1, NumItems)],
    {ok, Btree} = make_btree(Fd, KVs, ReduceFun),

    {_, Red, _} = cbt_btree:get_state(Btree),

    CopyCallback = fun(KV, Acc) -> {KV, Acc + 1} end,
    {ok, RootCopy, FinalAcc} = cbt_btree_copy:copy(
        Btree, CopyEts, [{backend, cbt_ets},
                         {before_kv_write, {CopyCallback, 0}}]),

    ?assertMatch(FinalAcc, length(KVs)),

    {ok, BtreeCopy} = cbt_btree:open(RootCopy, CopyEts, [{backend, cbt_ets},
                                                         {compression, none},
                                                         {reduce, ReduceFun}]),

    %% check copy
    {_, RedCopy, _} = cbt_btree:get_state(BtreeCopy),
    ?assertMatch(Red, RedCopy),
    {ok, _, CopyKVs} = cbt_btree:fold(
        BtreeCopy,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    ?_assertMatch(KVs, lists:reverse(CopyKVs)).

should_copy_compressed_btree(NumItems, {ReduceFun, _OriginalFileName, CopyEts,
                                        Fd}) ->

    KVs = [{I, I} || I <- lists:seq(1, NumItems)],
    {ok, Btree} = make_btree(Fd, KVs, ReduceFun, snappy),

    {_, Red, _} = cbt_btree:get_state(Btree),

    CopyCallback = fun(KV, Acc) -> {KV, Acc + 1} end,
    {ok, RootCopy, FinalAcc} = cbt_btree_copy:copy(Btree, CopyEts,
                                                   [{backend, cbt_ets},
                                                    {before_kv_write, {CopyCallback, 0}}]),

    ?assertMatch(FinalAcc, length(KVs)),

    {ok, BtreeCopy} = cbt_btree:open(RootCopy, CopyEts, [{backend, cbt_ets},
                                                         {compression, snappy},
                                                         {reduce, ReduceFun}]),

    %% check copy
    {_, RedCopy, _} = cbt_btree:get_state(BtreeCopy),
    ?assertMatch(Red, RedCopy),
    {ok, _, CopyKVs} = cbt_btree:fold(
        BtreeCopy,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    ?_assertMatch(KVs, lists:reverse(CopyKVs)).

make_btree(Fd, KVs, ReduceFun) ->
    make_btree(Fd, KVs, ReduceFun, none).

make_btree(Fd, KVs, ReduceFun, Compression) ->

    {ok, Btree} = cbt_btree:open(nil, Fd, [{compression, Compression},
                                           {reduce, ReduceFun}]),
    {ok, Btree2} = cbt_btree:add_remove(Btree, KVs, []),
    {_, Red, _} = cbt_btree:get_state(Btree2),
    ?assertMatch(Red, length(KVs)),
    ok = cbt_file:sync(Fd),
    {ok, Btree2}.
