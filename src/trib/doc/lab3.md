## Lab 3

Welcome to Lab 3. The goal of this lab is to take the bin storage that
we implemented in Lab 2 and make it fault-tolerant.

Lab 3 can be submitted in teams of up to 3 people.

## Get Your Repo Up-to-date

Hopefully no changes have been made, but just in case, update your repository.

```
$ cd ~/gopath/src/trib
$ git branch lab3
$ git checkout lab3
$ git pull /classes/cse223b/sp14/labs/trib lab3
```

This should be a painless update.

Note that we don't provide great unit tests to test fault tolerance (as it's
hard to spawn and kill processes from within unit tests). Make sure you test
this sufficiently using a testing mechanism of your own design.

## System Scale and Failure Model

There could be up to 300 backends. Backends may join and leave at will, but you
can assume that at any time there will be at least one backend online (so that
your system is functional). Your design is required to be fault-tolerant where
if there are at least three backends online at all times, there will be no
data loss.  You can assume that each backend join/leave event will have a time
interval of at least 30 seconds in between, and this time duration will be
enough for you to migrate storage.

There will be at least 1 and up to 10 keepers. Keepers may join and
leave at will, but at any time there will be at least 1 keeper online.
(Thus, if there is only one keeper, it will not go offline.) Also, you can
assume that each keeper join/leave event will have a time interval of
at least 1 minute in between. When a process 'leaves', assumee that the process is
killed-- everything in that process will be lost, and it will not have an
opportunity to clean up.

When keepers join, they join with the same `Index` as last time, although
they've lost any other state they may have saved. Each keeper will receive a new
`Id` in the `KeeperConfig`.

Initially, we will start at least one backend, and then at least one
keeper. At that point, the keeper should send `true` to the `Ready` channel and
a frontend should be able to issue `BinStorage` calls.

## Consistency Model

To tolerate failures, you have to save the data of each key in
multiple places. To keep things achievable, we have to slightly relax the
consistency model, as follows.

`Clock()` and the key-value calls (`Set()`, `Get()` and `Keys()`) will keep the
same semantics as before.

When concurrent `ListAppend()`s happen, calls to `ListGet()` might result in
values that are currently being added, and may appear in arbitrary order.
However, after all concurrent `ListAppend()`s return, `ListGet()` should always
return the list with a consistent order.

Here is an example of an valid call and return sequence:

- Initially, the list `"k"` is empty.
- A invokes `ListAppend("k", "a")`
- B invokes `ListAppend("k", "b")`
- C calls `ListGet("k")` and gets `["b"]`. Note that `"b"` appears first in the
  list here.
- D calls `ListGet("k")` and gets `["a", "b"]`, note that although
  `"b"` appeared first last time, it appears at the second position in
  the list now.
- A's `ListAppend()` call returns
- B's `ListAppend()` call returns
- C calls `ListGet("k")` again and gets `["a", "b"]`
- D calls `ListGet("k")` again and gets `["a", "b"]`

`ListRemove()` removes all matched values that are appended into
the list in the past, and sets the `n` field properly.
When (and only when) concurrent `ListRemove()` on the same key and
value is called, it is okay to 'double count' elements being removed.

`ListKeys()` keeps the same semantics.

## Entry Functions

The entry functions will remain exactly the same as they are in Lab 2. The only
thing that will change is that there may be multiple keepers listed in the
`KeeperConfig`.

## Additional Assumptions

- No network errors; when a TCP connection is lost (RPC client returning
  `ErrShutdown`), you can assume that the RPC server crashed.
- When a bin-client, backend, or keeper is killed, all data in that process will
  be lost; nothing will be carried over a respawn.
- It will take less than 20 seconds to read all data stored on a backend and
  write it to another backend.

## Requirements

- Although you might change how data is stored in the backends, your
  implementation should pass all past test cases, which means your system should
  be functional with a single backend.
- If there are at least three backends online, there should never be any data
  loss. Note that the set of three backends might change over time, so long as
  there are at least three at any given moment.
- Assuming there are backends online, storage function calls always return
  without error, even when a node and/or a keeper just joined or left.

## Building Hints

- You can use the logging techniques described in class to store everything (in
  lists on the backends, even for values).
- Let the keeper(s) keep track on the status of all the nodes, and do
  the data migration when a backend joins or leaves.
- Keepers should also keep track of the status of each other.

For the ease of debugging, you can maintain some log messages (by using `log`
package, or by writing to a TCP socket or a log file).  However, for the
convenience of grading, please turn them off by default when you turn in your
code.

Also, try to distribute yourselves evenly across the lab machines. If everyone
uses `vm143`, it'll be unhappy.

## Turning In

If you are submitting as a team, please create a file called `teammates` under
the root of `triblab` repo that lists the login ids of the members of your team,
each on its own line.

Make sure that you have committed every piece of your code (including the
`teammates` file) into the `triblab` repository. Then just type `make
turnin-lab3` under the root of your repository.

## Happy Lab 3. :-)
