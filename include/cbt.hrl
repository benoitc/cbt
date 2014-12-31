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

-define(DEFAULT_COMPRESSION, none).

-define(b2l(V), binary_to_list(V)).
-define(l2b(V), list_to_binary(V)).
-define(term_to_bin(T), term_to_binary(T, [{minor_version, 1}])).
-define(term_size(T),
    try
        erlang:external_size(T)
    catch _:_ ->
        byte_size(?term_to_bin(T))
    end).

-record(btree, {
    ref,
    mod,
    root,
    extract_kv = identity,  % fun({_Key, _Value} = KV) -> KV end,,
    assemble_kv =  identity, % fun({Key, Value}) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil,
    compression = ?DEFAULT_COMPRESSION,
    kv_chunk_threshold =  16#4ff,
    kp_chunk_threshold = 2 * 16#4ff
}).

-record(ets_btree_meta, {key,
                         write_loc=0}).

-record(ets_btree, {name,
                    root=nil}).

-record(ets_btree_data, {pos,
                         data}).


-define(ETS_META_KEY, '_db_meta_').
