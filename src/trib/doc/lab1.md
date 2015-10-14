## Lab 1

Welcome to Lab 1. The goal of this lab is to implement a key-value storage
service that can be called via RPC. In particular you need to:

1. Implement a key-value storage server type that wraps a `trib.Store`
   interface object and takes http RPC requests from the network.
2. Implement a key-value storage client type that fits `trib.Store`
   interface that relays all of its requests back to the server.

More specifically, you need to implement two entry functions that are
defined in the `triblab/lab1.go` file: `ServeBack()` and `NewClient()`.
Presently, they are both implemented with `panic("todo")`.

## Get Your Repo Up-to-date

While no major changes are planned to the `trib` library, it's a good idea to
make sure your repo is up-to-date none the less.

```
$ cd ~/gopath/src/trib
$ git pull origin master
$ cd ~/gopath/src/triblab
$ git pull origin master
```

The instructions here assume you used the the default directory setup. If you
did something else crazy, we assume you can figure out the appropriate
corrections. You can also ask the TA for help with merging.

## The Key-value Pair Service Interface

The goal of Lab 1 is to wrap a key-value pair interface with RPC. You don't need
to implement the key-value pair storage by yourself, but you need to use it
extensively in later labs, so it will be good for you to understand the service
semantics here.

The data structure and interfaces for the key-value pair service are defined in
the `trib/kv.go` file (in the `trib` repository). The main interface is
`trib.Storage`, which consists of three logical parts.

First is the key-string pair part, which is its own interface.

```
// Key-value pair interfaces
// Default value for all keys is empty string
type KeyString interface {
	// Gets a value. Empty string by default.
	Get(key string, value *string) error

	// Set kv.Key to kv.Value. Set succ to true when no error.
	Set(kv *KeyValue, succ *bool) error

    // List all the keys of non-empty pairs where the key matches the given
    // pattern.
	Keys(p *Pattern, list *List) error
}
```

`Pattern` is a (prefix, suffix) tuple. It has a `Match(string)` function
that returns true when the string matches has the prefix and suffix of the
pattern.

The second part is the key-list pair interface that handles list-valued
key-value pairs.

```
// Key-list interfaces.
// Default value for all lists is an empty list.
// After the call, list.L should never be nil.
type KeyList interface {
	// Get the list associated with 'key'.
	ListGet(key string, list *List) error

	// Append a string to the list. Set succ to true when no error.
	ListAppend(kv *KeyValue, succ *bool) error

	// Removes all elements that are equal to kv.Value in the list kv.Key.
	// n is set to the number of elements removed.
	ListRemove(kv *KeyValue, n *int) error

	// List all the keys of non-empty lists, where the key matches
	// the given pattern.
	ListKeys(p *Pattern, list *List) error
}
```

The `Storage` interface glues these two interfaces together, and also includes
an auto-incrementing clock feature:

```
type Storage interface {
    // Returns the value of an auto-incrementing clock. The return value will be
    // no smaller than atLeast, and it will be strictly larger than the value
    // returned last time the function was called, unless it was math.MaxUint64.
	Clock(atLeast uint64, ret *uint64) error

	KeyString
	KeyList
}
```

Note that the function signatures of these methods are already RPC-friendly.
You should implement the RPC interface with Go language's
[`rpc`](http://golang.org/pkg/net) package.  By doing this, another person's
client that speaks the same protocol will be able to talk to your server as
well.

Because of how the simple key-value store works, all the methods will always
return `nil` error when executed locally. Thus all errors you see from this
interface will be communication errors. You can assume that each call (on the
same key) is an atomic transaction; two concurrent writes won't give the key a
weird value that came from nowhere.  However, when an error occurs, the caller
won't know if the transaction committed or not, because the error might have
occured before or after the transaction executed on the server.

## Entry Functions

These are the two entry functions you need to implement for this Lab.  This is
how other people's code (and your own code in later labs) will use your code.

### Server-side

```
func ServeBack(b *trib.Back) error
```

This function creates an instance of a back-end server based on configuration
`b *trib.Back`. Structure `trib.Back` is defined in the `trib/config.go` file.
The struct has several fields:

- `Addr` is the address the server should listen on, in the form of
  `<host>:<port>`. Go uses this address in its
  [`net`](http://golang.org/pkg/net) package, so you should be able to use it
  directly on opening connections.
- `Store` is the storage device you will use for storing data.  You should not
  store persistent data anywhere else.  `Store` will never be nil.
- `Ready` is a channel for notifying the other parts in the program
  that the server is ready to accept RPC calls from the network (indicated by
  the server sending the value `true`) or if the setup failed (indicated by
  sending `false`). `Ready` might be nil, which means the caller does not care
  about when the server is ready.

This function should be a blocking call. It does not return until it experiences
an error (like the network shutting down).

Note that you don't need to (and should not) implement the key-value pair
storage service yourself.  You only need to wrap the given `Store` with RPC, so
that a remote client can access it via the network.

### Client-side

```
func NewClient(addr string) trib.Stroage
```

This function takes `addr` in the form of `<host>:<port>`, and connects to this
address for an http RPC server. It returns an implementation of `trib.Storage`,
which will provide the interface, and forward all calls as RPCs to the server.
You can assume that `addr` will always be a valid TCP address.

Note that when `NewClient()` is called, the server may not have started yet.
While it is okay to try to connect to the server at this time, you should not
report any error if your attempt fails.  It might be best to wait to establish
the connection until you need it to perform your first RPC function call.

## The RPC Package

Go language comes with its own [`net/rpc`](http://golang.org/pkg/net/rpc)
package in the standard library, and you will use that to complete this
assignment. Note that the `trib.Store` interface is already in "RPC friendly"
form.

Your RPC needs to use the default encoding `encoding/gob`, listen on the given
address, and serve as an http RPC server. The server needs to register the
back-end key-value pair object under the name `Storage`.

## Testing

Both the `trib` and `triblab` repository comes with a makefile with some handy
command line shorthands, and also some basic testing code.

Under the `trib` directory, if you type `make test`, you should see that the
tests run and all tests passed.

Under the `triblab` directory, if you type `make test-lab1`, you will see the
tests fail with a "todo panic" if you have not completed Lab 1 yet.

When you implement the logic behind Lab 1, you should pass these tests, and you
can be fairly confident that you'll get at least 30% of the credit for Lab 1
(assuming you're not cheating somehow).

However, the tests that come with the repository is fairly basic and simple.
Though you're not required to, you should consider writing more test cases to
make sure your implementation matches the specification.

For more information on writing test cases in Go, please read the
[testing](http://golang.org/pkg/testing/) package documentation.

## Starting Hints

While you are free to do the project in your own way as long as it fits the
specification, matches the interfaces, and passes the tests, here are some
suggested first steps.

First, create a `client.go` file under the `triblab` repo, and declare a new
struct called `client`:

```
package triblab

type client struct {
    // your private fields will go here
}
```

Then add method functions to this new `client` type so that it matches the
`trib.Storage` interface. For example, for the `Get()` function:

```
func (self *client) Get(key string, value *string) error {
    panic("todo")
}
```

After you've added all of the functions, you can add a line to force the
compiler to check if all of the functions in the interface have been
implemented:

```
var _ trib.Storage = new(client)
```

This creates a zero-filled `client` and assigns it to an anonymous variable of
type `trig.Storage`. Your code will thus only compile when your client satisfies
the interface. (Since this zero-filled variable is anonymous and nobody can
access it, it will be removed as dead code by the compiler's optimizer and hence
has no negative effect on the run-time execution.)

Next, add a field into `client` called `addr`, which will save the server
address.  Now `client` looks like this:

```
type client struct {
    addr string
}
```

Now that we have a client type that satisfies `trib.Storage`, we can return this
type in our entry function `NewClient()`. Remove the `panic("todo")` line in
`NewClient()`, and replace it by returning a new `client` object. Now the
`NewClient()` function should look something like this:

```
func NewClient(addr string) trib.Storage {
    return &client{addr: addr}
}
```

Now all you need to do for the client half is to fill in the code skeleton with
the correct RPC logic.

To do an RPC call, we need to import the `rpc` package, so at the start of the
`client.go` file, let's import `rpc` after the package name statement.

```
import (
    "net/rpc"
)
```

The examples in the `rpc` package show how to write the basic RPC client logic.
Following their example, you might create a `Get()` method that looks something
like this:

```
func (self *client) Get(key string, value *string) error {
    // connect to the server
    conn, e := rpc.DialHTTP("tcp", self.addr)
    if e != nil {
        return e
    }

    // perform the call
    e = conn.Call("Storage.Get", key, value)
    if e != nil {
        conn.Close()
        return e
    }

    // close the connection
    return conn.Close()
}
```

However, if you do it this way, you will open a new HTTP connection for every
RPC call. This approach is acceptable but obviously not the most efficient way
available to you.  We leave it to you to figure out how to maintain a
persistent RPC connection, if it's something you want to tackle.

Once you've completed the client side, you also need to wrap the server side in the
`ServeBack()` function using the same `rpc` library. This should be pretty
straight-forward if you follow the example server in the RPC documentation. You
do this by creating an RPC server, registering the `Store` member field in the
`b *trib.Config` parameter under the name `Storage`, and create and start an
HTTP server.  Just remember that you need to register as `Storage` and also need
to send a `true` over the `Ready` channel when the service is ready (when
`Ready` is not `nil`), and send a `false` when you encounter any error on
starting your service.

When all of these changes are done, you should pass the test cases written in
the `back_test.go` file. It calls the `CheckStorage()` function defined in the
`trib/tribtest` package, and performs some basic checks to see if an RPC client
and a server (that runs on the same host) will satisfy the specification of a
key-value pair service (as a local `trib/store.Storage` does without RPC).

## Playing with your implementation

To do some simple testing with your own implementation, you can use the
`kv-client` and `kv-server` command line utilities.

First make sure your code compiles.

Then run the server.

```
$ kv-server
```

*(You might need to add `$GOPATH/bin` to your `$PATH` to run this.)*

You should see an address print out (e.g. `localhost:12086`).  By default, the
server will choose an address of the form `localhost:rand`. If desired, you can
override this setting with a command line flag.

Now you can play with your server via the `kv-client` program.
For example:

```
$ kv-client localhost:12086 get hello

$ kv-client localhost:12086 set foo value
true
$ kv-client localhost:12086 get foo
value
$ kv-client localhost:12086 keys fo
foo
$ kv-client localhost:12086 list-get hello
$ kv-client localhost:12086 list-get foo
$ kv-client localhost:12086 list-append foo something
true
$ kv-client localhost:12086 list-get foo
something
$ kv-client localhost:12086 clock
0
$ kv-client localhost:12086 clock
1
$ kv-client localhost:12086 clock
2
$ kv-client localhost:12086 clock 200
200
```

## Requirements

- When the network and storage are errorless, RPC to your server should never
  return an error.
- When the network has an error (like the back-end server crashed, and thus the
  client cannot connect), your RPC client should return an error.  As soon
  as the server is back up and running, your RPC client should act as normal
  again (without needing to create a new client).
- When the server and the clients are running on the lab machines,
  your RPC should introduce less than 100 milliseconds of additional latency.

## Turning In

First, make sure that you have committed every piece of your code into
the repository `triblab`. Then just type `make turnin-lab1` under the root
of the repository.  The script will generate a `turnin.zip` file that contains
everything in your git repository, and then copy into an appropriate location.

## Happy Lab 1!
