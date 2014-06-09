#cbt

multi-layer MVCC log append-only database library based on the Apache CouchDB btree.

## Changes compared to couchdb

- use CRC32 to check data integrity instead of MD5
- rewrote the file init part to make it more robust
- removed the need of an external config. Provides correct default and use erlang environment.
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

    $ git clone git@bitbucket.org:refugeio/cbt.git

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
[online](http://refugeio.bitbucket.org/cbt/index.html).

## Example of usage:

Example of usage:

Store a {Key Value} pair in a btree:

    1> {ok, Fd} = cbt_file:open("test.db", [create_if_missing]).   
    {ok,<0.35.0>}
    2> {ok, Btree} = cbt_btree:new(Fd).
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

## contribute

Open Issues and Support tickets in [Jira](https://issues.refuge.io/browse/CBT
).
Code is available on [bitbucket](https://bitbucket.org/refugeio/cbt).