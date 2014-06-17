-module(cbt_btree_copy_tests).

-include_lib("include/cbt.hrl").
-include("cbt_tests.hrl").


setup_copy(_) ->
    ReduceFun = fun(reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,

    OriginalFileName = ?tempfile(),
    CopyFileName = OriginalFileName ++ ".copy",
    {ok, Fd} = cbt_file:open(OriginalFileName, [create, overwrite]),
    {ok, FdCopy} = cbt_file:open(CopyFileName, [create, overwrite]),
    {ReduceFun, OriginalFileName, CopyFileName, Fd, FdCopy}.

teardown(_, {_, OriginalFileName, CopyFileName, Fd, FdCopy}) ->
    ok = cbt_file:close(Fd),
    ok = cbt_file:close(FdCopy),
    ok = file:delete(OriginalFileName),
    ok = file:delete(CopyFileName).

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

should_copy_btree(NumItems, {ReduceFun, _OriginalFileName, _CopyFileName,
                             Fd, FdCopy}) ->
    KVs = [{I, I} || I <- lists:seq(1, NumItems)],
    {ok, Btree} = make_btree(Fd, KVs, ReduceFun),

    {_, Red, _} = cbt_btree:get_state(Btree),

    CopyCallback = fun(KV, Acc) -> {KV, Acc + 1} end,
    {ok, RootCopy, FinalAcc} = cbt_btree_copy:copy(
        Btree, FdCopy, [{before_kv_write, {CopyCallback, 0}}]),

    ?assertMatch(FinalAcc, length(KVs)),

    {ok, BtreeCopy} = cbt_btree:open(
        RootCopy, FdCopy, [{compression, none}, {reduce, ReduceFun}]),

    %% check copy
    {_, RedCopy, _} = cbt_btree:get_state(BtreeCopy),
    ?assertMatch(Red, RedCopy),
    {ok, _, CopyKVs} = cbt_btree:fold(
        BtreeCopy,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    ?_assertMatch(KVs, lists:reverse(CopyKVs)).


make_btree(Fd, KVs, ReduceFun) ->
    {ok, Btree} = cbt_btree:open(nil, Fd, [{compression, none},
                                           {reduce, ReduceFun}]),
    {ok, Btree2} = cbt_btree:add_remove(Btree, KVs, []),
    {_, Red, _} = cbt_btree:get_state(Btree2),
    ?assertMatch(Red, length(KVs)),
    ok = cbt_file:sync(Fd),
    {ok, Btree2}.
