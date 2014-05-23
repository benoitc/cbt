-module(cbt).
-behaviour(gen_server).

-export([open/2, open/3, open/4,
         open_link/2, open_link/3, open_link/4,
         close/1]).

-export([transact/2, transact/3]).
-export([add/2,
         add_remove/3,
         query_modify/4,
         lookup/2,
         fold/3, fold/4,
         size/1,
         full_reduce/1,
         final_reduce/2,
         fold_reduce/4]).


-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-export([start_app/0]).
-export([get_updater/1]).

-include("cbt.hrl").

-ifdef(DEBUG).
-define(log(Fmt,Args),io:format(user,Fmt,Args)).
-else.
-define(log(Fmt,Args),ok).
-endif.

-define(DEFAULT_TIMEOUT, 15000).



%% PUBLIC API
%%
-type cdb() :: pid().

-type config_option() :: {compress, none | gzip | snappy | lz4}
                       | {page_size, pos_integer()}
                       | {read_buffer_size, pos_integer()}
                       | {write_buffer_size, pos_integer()}
                       | {merge_strategy, fast | predictable }
                       | {sync_strategy, none | sync | {seconds, pos_integer()}}
                       | {expiry_secs, non_neg_integer()}
                       | {spawn_opt, list()}.

-type config_options() :: [config_option()].

-type btree_spec() :: {term(), list()}.


%% @doc
%% Create or open a cdb store. Argument `Dir' names a
%% directory in which to keep the data files. By convention, we
%% name cdb data directories with extension ".cdb".
- spec open(Dir::string(), Specs::[btree_spec()])
    -> {ok, cdb()} | ignore | {error, term()}.
open(Dir, BtreeSpecs) ->
    open(Dir, BtreeSpecs, []).

%% @doc Create or open a cdb store.
- spec open(Dir::string(), Specs::[btree_spec()], Opts::[config_option()])
    ->  {ok, cdb()} | ignore | {error, term()}.
open(Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt_util:get_opt(spawn_opt, Opts, []),
    gen_server:start(?MODULE, [Dir, BtreeSpecs, Opts], [{spawn_opt,SpawnOpt}]).

%% @doc Create or open a cdb store with a registered name.
- spec open(Name::{local, atom()} | {global, term()} | {via, term()},
            Dir::string(), Specs::[btree_spec()], Opts::config_options())
    -> {ok, cdb()} | ignore | {error, term()}.
open(Name, Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt:get_opt(spawn_opt, Opts, []),
    gen_server:start(Name, ?MODULE, [Dir, BtreeSpecs, Opts], [{spawn_opt,SpawnOpt}]).

%% @doc
%% Create or open a cdb store as part of a supervision tree.
%% Argument `Dir' names a directory in which to keep the data files.
%% By convention, we name cdb data directories with extension
%% ".cdb".
- spec open_link(Dir::string(), Specs::[btree_spec()])
    -> {ok, cdb()} | ignore | {error, term()}.
open_link(Dir, BtreeSpecs) ->
    open_link(Dir, BtreeSpecs, []).

%% @doc Create or open a cdb store as part of a supervision tree.
- spec open_link(Dir::string(), Specs::[btree_spec()],
                 Opts::[config_option()])
    -> {ok, cdb()} | ignore | {error, term()}.
open_link(Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt:get_opt(spawn_opt, Opts, []),
    gen_server:start_link(?MODULE, [Dir, BtreeSpecs, Opts],
                          [{spawn_opt,SpawnOpt}]).

%% @doc Create or open a cdb store as part of a supervision tree
%% with a registered name.
- spec open_link(Name::{local, atom()} | {global, term()} | {via, term()},
                 Dir::string(),  Specs::[btree_spec()],
                 Opts::[config_option()])
    -> {ok, cdb()} | ignore | {error, term()}.
open_link(Name, Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt_util:get_opt(spawn_opt, Opts, []),
    gen_server:start_link(Name, ?MODULE, [Dir, BtreeSpecs, Opts],
                          [{spawn_opt,SpawnOpt}]).

%% @doc
%% Close a cdb data store.
- spec close(Ref::pid()) -> ok.
close(Ref) ->
    try
        gen_server:call(Ref, close, infinity)
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok
    end.


transact(Ref, TransactFun) ->
    transact(Ref, TransactFun, []).

transact(Ref, TransactFun, Args) ->
    {ok, UpdaterPid} = get_updater(Ref),
    Tag = erlang:monitor(process, UpdaterPid),
    UpdaterPid ! {transact, TransactFun, Args, self(), Tag},

    try
        erlang:send(UpdaterPid, {transact, TransactFun, Args, self(), Tag},
                      [noconnect]),
        receive
            {Tag, Resp} ->
                Resp;
            {'DOWN', Tag, _, _, Reason} ->
                error_logger:error_msg("updater pid exited with reason ~p~n",
                                       [Reason]),

                {error, updater_exited, Reason}
        end

    after
        erlang:demonitor(Tag, [flush])
    end.



add(BtName, InsertKeyValues) ->
    add_remove(BtName, InsertKeyValues, []).

add_remove(BtName, InsertKeyValues, RemoveKeys) ->
    {ok, []} = query_modify(BtName, [], InsertKeyValues, RemoveKeys),
    ok.

query_modify(BtName, LookupKeys, InsertValues, RemoveKeys) ->
    if_trans(BtName, fun(Btree, Db) ->
                {ok, QueryResults, Btree2} = cbt_btree:query_modify(
                        Btree, LookupKeys, InsertValues, RemoveKeys),
                NewDb = set_btree(BtName, Btree2, Db),
                erlang:put(cbt_trans, NewDb),
                {ok, QueryResults}
        end).


lookup(BtName, Keys) ->
    if_trans(BtName, fun(Btree, _Db) ->
                cbt_btree:lookup(Btree, Keys)
        end).


fold(BtName, Fun, Acc) ->
    fold(BtName, Fun, Acc, []).

fold(BtName, Fun, Acc, Options) ->
    if_trans(BtName, fun(Btree, _Db) ->
                cbt_btree:fold(Btree, Fun, Acc, Options)
        end).

size(BtName) ->
    if_trans(BtName, fun(Btree, _Db) ->
                cbt_btree:size(Btree)
        end).

final_reduce(BtName, Val) ->
    if_trans(BtName, fun(Btree, _Db) ->
                cbt_btree:final_reduce(Btree, Val)
        end).


fold_reduce(BtName, Fun, Acc, Options) ->
    if_trans(BtName, fun(Btree, _Db) ->
                cbt_btree:fold_reduce(Btree, Fun, Acc, Options)
        end).


full_reduce(BtName) ->
    if_trans(BtName, fun(Btree, _Db) ->
                cbt_btree:full_reduce(Btree)
        end).


%% @doc get updater pid
get_updater(Ref) ->
    gen_server:call(Ref, get_updater, infinity).


%% gen_server api
%%
%%

init([Dir, BtreeSpecs, Options]) ->
    case cbt_file:open(Dir, Options) of
        {ok, Fd} ->
            {ok, UpdaterPid} = cbt_updater:start_link(self(), Fd, Dir,
                                                      BtreeSpecs,
                                                      Options),
            Db = cbt_updater:get_state(UpdaterPid),
            process_flag(trap_exit, true),
            {ok, Db};
        Error ->
            Error
    end.

handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};

handle_call({db_updated, Db}, _From, _OldDb) ->
    {reply, ok, Db};

handle_call(get_updater, _From, #db{updater_pid=UpdaterPid}=Db) ->
    {reply, {ok, UpdaterPid}, Db};

handle_call(close, _From, Db) ->
    {stop, normal, ok, Db}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};

handle_info({'EXIT', Fd, Reason}, #db{fd=Fd}=Db) ->
    error_logger:error_msg("file crashed with reason ~p~n",
                           [Reason]),
    {stop, {error, Reason}, Db};

handle_info({'EXIT', Pid, Reason}, #db{updater_pid=Pid}=Db) ->
    error_logger:error_msg("updater pid crashed with reason ~p~n",
                           [Reason]),

    #db{fd=Fd,
        dir=Dir,
        btree_specs=BtreeSpecs,
        options=Options} = Db,


    case cbt_updater:start_link(self(), Fd, Dir, BtreeSpecs, Options) of
        {ok, UpdaterPid} ->
            NewDb = cbt_updater:get_state(UpdaterPid),
            {noreply, NewDb};
        Error ->
            {stop, Error, Db}
    end;



handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #db{fd=Fd, updater_pid=UpdaterPid}) ->
    ok =  cbt_updater:stop(UpdaterPid),
    ok =  cbt_file:close(Fd),

    ok.


%% UTILS fonctions
start_app() ->
    {ok, _} = cbt_util:ensure_all_started(cbt),
    ok.


%% transaction utilities
get_btree(Name, #db{btrees=Btrees}) ->
    case proplists:get_value(Name, Btrees) of
        undefined ->
            false;
        Btree ->
            Btree
    end.

set_btree(Name, Btree, #db{btrees=Btrees}=Db) ->
    Btrees2 = lists:keyreplace(Name, 1, Btrees, {Name, Btree}),
    Db#db{btrees=Btrees2}.


if_trans(BtName, Fun) ->
    case cbt_updater:if_transaction() of
        {true, Db} ->
            case get_btree(BtName, Db) of
                false ->
                    unknown_btree;
                Btree ->
                    Fun(Btree, Db)
            end;
        _ ->
            no_transaction
    end.
