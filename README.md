#cbt

multi-layer MVCC log append-only database library based on the Apache CouchDB btree.

## Changes compared to couchdb

- Pluggable Storage backends
- use CRC32 to check data integrity instead of MD5
- rewrote the file init part to make it more robust
- removed the need of an external config. Provides correct default and use
  erlang environment.
- documentation and specs
- some syntax improvements.

## build

### 1. install rebar
To build cbt you need to install rebar in your `PATH`. Rebar is
available on Github:

https://github.com/rebar/rebar

Follow the
[README](https://github.com/rebar/rebar/blob/master/README.md) to
install it.

### 2. build

Fetch the source code:

    $ git clone https://bitbucket.org/refugeio/cbt.git

Build the source, run the `make` command. It will fetch any needed
dependencies.

    $ cd /<PATH_TO>/cbt
    $ make

### 3. test CBT

Run the following command line:

    $ make test


### 3. Build the doc

    $ make doc

and open the `index.html` file in the doc folder. Or read it
[online](http://cbt.cowdb.org).

## Example of usage with the file backend

Example of usage:

Store a {Key Value} pair in a btree:

    1> {ok, Fd} = cbt_file:open("test.db", [create_if_missing]).
    {ok,<0.35.0>}
    2> {ok, Btree} = cbt_btree:open(nil, Fd).
    {ok,{btree,<0.35.0>,nil,undefined,undefined,undefined,nil,
               snappy,1279}}
    3>
    3> {ok, Btree2} = cbt_btree:add(Btree, [{a, 1}]).
    {ok,{btree,<0.35.0>,
               {0,[],32},
               undefined,undefined,undefined,nil,snappy,1279}}
    4> Root = cbt_btree:get_state(Btree2).
    {0,[],32}
    5> Header = {1, Root}.
    {1,{0,[],32}}
    6> cbt_file:write_header(Fd, Header).
    ok

What we did here is to open a file, create a btree inside and add a key
value. Until we write the header, the database value is not changed.

Now open the database in a new process and read the btree using the last
header:

    7> {ok, Fd1} = cbt_file:open("test.db").
    {ok,<0.44.0>}
    8>
    8> {ok, Header1} = cbt_file:read_header(Fd1).
    {ok,{1,{0,[],32}}}
    9> Header1 == Header
    9> .
    true
    10> {_, ReaderRoot} = Header1.
    {1,{0,[],32}}
    11> {ok, SnapshotBtree} = cbt_btree:open(ReaderRoot, Fd1).
    {ok,{btree,<0.44.0>,
               {0,[],32},
               undefined,undefined,undefined,nil,snappy,1279}}
    12> cbt_btree:lookup(SnapshotBtree, [a]).
    [{ok,{a,1}}]

You can check that the database value is not change until we store the
header:

    13> {ok, Btree4} = cbt_btree:add(Btree2, [{a, 1}, {b, 2}]).
    {ok,{btree,<0.35.0>,
               {4160,[],39},
               undefined,undefined,undefined,nil,snappy,1279}}
    14> cbt_btree:lookup(Btree4, [a, b]).
    [{ok,{a,1}},{ok,{b,2}}]
    15> Root2 = cbt_btree:get_state(Btree4).
    {4160,[],39}
    16> Header2 = {1, Root2}.
    {1,{4160,[],39}}
    17> cbt_file:write_header(Fd, Header2).
    ok
    18> cbt_btree:lookup(SnapshotBtree, [a, b]).
    [{ok,{a,1}},not_found]


## ETS backend

Find here a simple usage of the ETS backend of cbt allowing you to store one
database in an ETS.

    1> cbt_ets:new(test).
    test
    2> {ok, Bt} = cbt_ets:open_btree(test, test).
    {ok,{btree,test,cbt_ets,nil,identity,identity,
               #Fun<cbt_btree.1.30772535>,nil,none,1279,2558}}
    3> {ok, Bt2} = cbt_btree:add(Bt, [{a, 1}]).
    {ok,{btree,test,cbt_ets,
               {1,[],28},
               identity,identity,#Fun<cbt_btree.1.30772535>,nil,none,1279,
               2558}}
    4>  cbt_ets:update_btree(test, test, Bt2).
    true
    5> {ok, SnapshotBtree} = cbt_ets:open_btree(test, test).
    {ok,{btree,test,cbt_ets,
               {1,[],28},
               identity,identity,#Fun<cbt_btree.1.30772535>,nil,none,1279,
               2558}}
    6> cbt_btree:lookup(SnapshotBtree, [a]).
    [{ok,{a,1}}]
    7> {ok, Bt3} = cbt_btree:add(Bt2, [{b, 2}]).
    {ok,{btree,test,cbt_ets,
               {2,[],36},
               identity,identity,#Fun<cbt_btree.1.30772535>,nil,none,1279,
               2558}}
    8> cbt_ets:update_btree(test, test, Bt3).
    true
    9> cbt_btree:lookup(SnapshotBtree, [a, b]).
    [{ok,{a,1}},not_found]
    10> {ok, SnapshotBtree2} = cbt_ets:open_btree(test, test).
    {ok,{btree,test,cbt_ets,
               {2,[],36},
               identity,identity,#Fun<cbt_btree.1.30772535>,nil,none,1279,
               2558}}
    11> cbt_btree:lookup(SnapshotBtree2, [a, b]).
    [{ok,{a,1}},{ok,{b,2}}]i

## Custom storage backend

CBT provides you 2 different backends by default:

- `cbt_file`: Backend to store data in a file
- `cbt_ets`: Backend to store data in ETS.

But can use a custom backends to store the btree data if you need it. For
example if you want to store the btree in a custom file backend when you want
to change the data types or when you want to store the BTREE over a Key/Value
store.

To do it just pass the backend module to the btree and give it the Reference
(atom or pid) that have been created when initializing the backend. Have a
look in the `cbt_ets' module for more informations.
