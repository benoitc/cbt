-module(cbt_updater).
-behaviour(gen_server).

-export([start_link/5]).

-export([if_transaction/0]).
-export([get_state/1]).
-export([stop/1]).


-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).


-include("cbt.hrl").

if_transaction() ->
    case erlang:get(cbt_trans) of
        #db{}=Db ->
            {true, Db};
        _ ->
            false
    end.



get_state(Pid) ->
    gen_server:call(Pid, get_db, infinity).

stop(Pid) ->
    try
        gen_server:call(Pid, stop, infinity)
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok
    end.


start_link(DbPid, Fd, Dir, BtreeSpecs, Options) ->
    gen_server:start_link(?MODULE, [DbPid, Fd, Dir, BtreeSpecs,
                                    Options], []).

init([DbPid, Fd, Dir, BtreeSpecs, Options]) ->
    CompactDir = cbt_util:get_opt(compact_dir, Options,
                                  filename:dirname(Dir)),
    FileName = filename:basename(Dir),
    {ok, Header} = case lists:member(create, Options) of
        true ->
            Header1 = #db_header{},
            ok = cbt_file:write_header(Fd, Header1),
            cbt_file:delete(CompactDir, FileName ++
                            ".compact"),
            {ok, Header1};
        false ->
            case cbt_file:read_header(Fd) of
                {ok, Header1} -> {ok, Header1};
                no_valid_header ->
                    Header1 = #db_header{},
                    ok = cbt_file:write_header(Fd, Header1),
                    cbt_file:delete(CompactDir, FileName ++
                                    ".compact"),
                    {ok, Header1}
            end
    end,
    Db = init_db(DbPid, Fd, Header, Dir, BtreeSpecs, Options),
    {ok, Db}.


handle_call(get_db, _From, Db) ->
    {reply, Db, Db};

handle_call(stop, _From, Db) ->
    {stop, normal, ok, Db}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({transact, TFun, Arg, From, Tag}, #db{db_pid=DbPid,
                                                  changes=N}=Db) ->
    {Resp, Db2} = do_transact(TFun, Arg, Db),

    %% if the database has been updated tell it to the main db object.
    if Db2#db.changes > N ->
            ok = gen_server:call(DbPid, {db_updated, Db2});
        true -> ok
    end,

    From ! {Tag, Resp},

    {noreply, Db2};

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _Db) ->
    ok.

init_db(DbPid, Fd, Header, Dir, BtreeSpecs, Options) ->
    case element(2, Header) of
        ?LATEST_DISK_VERSION ->
            DefaultFSyncOptions = [before_header, after_header, on_file_open],
            FSyncOptions = cbt_util:get_opt(fsync_options, Options,
                                            DefaultFSyncOptions),

            case lists:member(on_file_open, FSyncOptions) of
                true ->
                    ok = cbt_file:sync(Fd);
                _ ->
                    ok
            end,

            %% initialise btrees using the btree spec
            Btrees = case Header#db_header.btrees of
                [] ->
                    Btrees1 = lists:foldl(fun({Name, Opts}, Acc) ->
                                {ok, Btree} = cbt_btree:open(nil, Fd, Opts),
                                [{Name, Btree} | Acc]
                            end, [], BtreeSpecs),
                    lists:reverse(Btrees1);
                States ->
                    Btrees1 = lists:foldl(fun({Name, St}, Acc) ->
                                    Opts = proplists:get_value(Name, BtreeSpecs,
                                                              []),
                                    {ok, Btree} = cbt_btree:open(St, Fd, Opts),
                                    [{Name, Btree} |Acc]
                            end, [], States),
                    lists:reverse(Btrees1)
            end,

            #db{db_pid = DbPid,
                updater_pid = self(),
                fd = Fd,
                changes = Header#db_header.changes,
                btrees = Btrees,
                meta = Header#db_header.meta,
                dir = Dir,
                options = Options,
                fsync_options = FSyncOptions};
        _ ->
            {error, database_disk_version_error}
    end.


do_transact(TFun, Args, Db) ->
    erlang:put(cbt_trans, Db),
    try
        Resp = apply(TFun, Args),
        Db1 = erlang:get(cbt_trans),
        {ok, Db2} = if Db /= Db1 ->
                commit(Db1);
            true -> {ok, Db}
        end,
        {Resp, Db2}
    after
        erlang:erase(cbt_trans)
    end.



commit(#db{fd=Fd, changes=N}=Db) ->
    Header = make_header(Db),
    ok = cbt_file:write_header(Fd, Header),
    {ok, Db#db{changes=N+1}}.

make_header(#db{changes=N, btrees=Btrees, meta=Meta}) ->
    %% we only store the roots in the header.
    Roots = [{Name, cbt_btree:get_state(Bt)} || {Name, Bt} <- Btrees],

    %% return the new header
    #db_header{changes = N + 1,
               btrees = Roots,
               meta = Meta}.
