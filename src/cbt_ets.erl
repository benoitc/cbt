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

-module(cbt_ets).
-behaviour(cbt_backend).

-include("cbt.hrl").

%% public API
-export([new/1, delete/1]).
-export([open_btree/2, open_btree/3,
         update_btree/3,
         delete_btree/2,
         bytes/1]).

%% backend API
%%
-export([append_term/2, append_term/3,
         pread_term/2,
         sync/1,
         empty/1]).

new(DbName) when is_atom(DbName) ->
    Tid = ets:new(DbName, [named_table, ordered_set, public, {keypos, 2}]),
    %% make meta
    ets:insert_new(DbName, #ets_btree_meta{key=?ETS_META_KEY}),
    Tid.

delete(Tab) ->
   true = ets:delete(Tab),
   ok.


open_btree(Tab, BtName) ->
    open_btree(Tab, BtName, []).

open_btree(Tab, BtName, Options0) when Tab /= ?ETS_META_KEY ->
    Options = [{backend, cbt_ets}] ++ Options0,
    case ets:lookup(Tab, BtName) of
        [] ->
            %% create a new btree if missing
            cbt_btree:open(nil, Tab, Options);
        [#ets_btree{root=BtState}] ->
            %% reopen the btree
            cbt_btree:open(BtState, Tab, Options)
    end.

update_btree(Tab, BtName, Btree) ->
    BtState = cbt_btree:get_state(Btree),
    ets:insert(Tab, #ets_btree{name=BtName, root = BtState}).

delete_btree(Tab, BtName) ->
    ets:delete(Tab, BtName).

bytes(Tab) ->
    ets:info(Tab, memory).

%% BACKEND API
%%

append_term(Tab, Term) ->
    append_term(Tab, Term, []).


append_term(Tab, Term, Options) ->
    % compress term
    Comp = cbt_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    Data = cbt_compress:compress(Term, Comp),
    NewPos = ets:update_counter(Tab, ?ETS_META_KEY,
                                {#ets_btree_meta.write_loc, 1}),
    ets:insert(Tab, #ets_btree_data{pos=NewPos, data=Data}),
    {ok, NewPos, byte_size(Data)}.

pread_term(Tab, Pos) ->
    case ets:lookup(Tab, Pos) of
        [] -> {missing_btree_item, Pos};
        [#ets_btree_data{data=Bin}] -> {ok, cbt_compress:decompress(Bin)}
    end.

sync(_Tab) ->
    ok.

empty(Tab) ->
    %% delete all objects in the table
    ets:delete_all_objects(Tab),
    %% reiitialize the meta data
    ets:new(Tab, [named_table, ordered_set, public, {keypos, 2}]),
    ets:insert_new(Tab, #ets_btree_meta{key=?ETS_META_KEY}),
    ok.
