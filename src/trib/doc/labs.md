## Machines

We have set up a cluster of 10 machines. You should use these for all of the lab
assignments:

<ul id="machine_list"></ul>
<script>
function shuffle(array) {
    var currentIndex = array.length, temporaryValue, randomIndex ;
    while (0 !== currentIndex) {
        randomIndex = Math.floor(Math.random() * currentIndex);
        currentIndex -= 1;
        temporaryValue = array[currentIndex];
        array[currentIndex] = array[randomIndex];
        array[randomIndex] = temporaryValue;
    }
    return array;
}
vms = ["vm143", "vm144", "vm145", "vm146", "vm147",
       "vm148", "vm149", "vm150", "vm151", "vm152"];
vms = shuffle(vms);

for (var i = 0; i < vms.length; ++i) {
    var vm = vms[i];
    var node = document.createElement("li");
    var textnode = document.createTextNode(vm + ".sysnet.ucsd.edu");
    node.appendChild(textnode);
    document.getElementById("machine_list").appendChild(node);
}
</script>

They are all available exclusively via SSH.

## Programming Language

You will write the labs in Google's [golang](http://golang.org).  It is a young
language with a syntax somewhere between C/C++ and Python. It comes with a very
rich standard library, and language-level support for light-weight but powerful
concurrency semantics with *go routines* and *channels*.

Here is some key documentation on the language:

- [Go Language Documentation Page](http://golang.org/doc/)
- [Effective Go](http://golang.org/doc/effective_go.html)
- [Go Language Spec](http://golang.org/ref/spec)
- [Go Tutorial](https://tour.golang.org/)

You should be able to find a lot of documents about the Go language on the web,
especially from the official site. We highly recommend the official Go tutorial
(or "Tour") linked above.

- Go code is organized into many separate *packages*.
- Unlike C/C++, when defining a *variable*, the *type* of it is written after
  the variable name.
- Go language has pointers, but has no pointer arithmetic. For example, you
  cannot increase a pointer by 1 to point the next element in memory.
- Go language has fixed length *arrays*, but most of the time people
  use *slices*, which are sliced views of an underlying array that are
  often implicitly declared. Slices feel very much like Python lists.
- *maps* are built-in hash-based dictionaries.
- A function can have multiple return values.
- Exceptions are called `panic` and `recover`. However `panic` should only be
  used in dire cases. Error handling should be done with returned `Error`
  structs.
- `for` is the only looping mechanism.
- *Foreach* is implemented with the `range` keyword.
- Semicolons at the end of statements are optional, but discouraged.
- Variables are garbage collected. The language is hence type safe and pointer
  safe. When you have a pointer, the content it points to is always valid.
- Any identifier that starts with a capital letter is *public* and visible to
  other packages; others are *private* and only visible inside its own package.
- *Inheritance* is done by compositions of anonymous members.
- Virtual functions are bound via *interfaces*. Unlike Java, *interface* does
  not require explicit binding (via the *implements* keyword). As long as the
  type has the set of methods implemented, it can be automatically assigned to
  an inteface. As a result, it is okay to write the implementation first and
  declare the interface afterwards.
- Circular package dependency is not allowed.

## The Tribbler Story

Some cowboy programmer wrote a simple online microblogging service called
Tribbler and, leveraging the power of the Web, it becomes quite popular.
However, the program runs in a single process on a single machine; it does not
scale, cannot support many concurrent connections, and is vulnerable to machine
crashes. Knowing that you are taking the distributed computing system course at
UCSD, he asks you for help. You answered his call and are starting work on this
project.

Your goal is to refactor Tribbler into a distributed system, making it more
robust and scalable.

## Getting Started

The Tribbler project is written in golang and stored in a git repository. To
get started, run these commands from the command line on one of the course
machines:

```
$ cd                       # go to your home directory
$ mkdir -p gopath/src      # the path you use for storing golang src
$ cd gopath/src
$ git clone /class/labs/trib
$ git clone /class/labs/triblab
$ export GOPATH=~/gopath
$ go install ./...
```

You can do some basic testing to see if the framework is in good shape:

```
$ go test ./trib/...
```

The basic Tribbler service should now be installed on the system from your home
directory. Let's give it a try:

```
$ ~/gopath/bin/trib-front -init -addr=:rand
```

The program should show the URL it is running under (it uses a randomly
generated port).

Open your browser and type in the given address. For example, if the machine you
logged into was `vm151.sysnet.ucsd.edu`, and Tribbler is running on port 27944,
then open `http://vm151.sysnet.ucsd.edu:27944`.  You should see a list of
Tribbler users. You can view their tribs and login as them (with no
authentication).

This is how Tribbler looks to users.  It is a single web page that performs AJAX
calls (a type of web-based RPC) to the back-end web server. The webserver then in
turn calls the Tribbler logic functions and returns the results back to the Web
page in the browser.

If you find it difficult to access the lab machines outside of UCSD's campus,
you need to setup the UCSD VPN or use an SSH tunnel. Information about the
former is available
[here](http://blink.ucsd.edu/technology/network/connections/off-campus/VPN/).

## Source Code Organization


The source code in the `trib` package repository is organized as
follows:

- `trib` defines the common Tribbler interfaces and data structures.
- `trib/tribtest` provides several basic test cases for the interfaces.
- `trib/cmd/trib-front` is the web-server launcher that you run.
- `trib/cmd/kv-client` is a command line key-value RPC client for quick testing.
- `trib/cmd/kv-server` runs a key-value service as an RPC server.
- `trib/cmd/bins-client` is a bin storage service client.
- `trib/cmd/bins-back` is a bin storage service back-end launcher.
- `trib/cmd/bins-keeper` is a bin stroage service keeper launcher.
- `trib/cmd/bins-mkrc` generates a bin storage configuration file.
- `trib/entries` defines several helper functions for constructing a Tribbler
  front-end or a back-end.
- `trib/ref` is a reference monolithic implementation of the `trib.Server`
  interface. All the server logic runs in one single process.  It is not
  scalable and is vulnerable to machine crashes.
- `trib/store` contains an in-memory thread-safe implementation of the
  `trib.Store` interface. We will use this as the basic building block for our
  back-end storage system.
- `trib/randaddr` provides helper functions that generate a network
  address with a random port number.
- `trib/local` provides helper functions that check if an address
  belongs to the machine that the program is running.
- `trib/colon` provides helper functions that escape and unescape colons in a
  string.
- `trib/www` contains the static files (html, css, js, etc.) for the web
  front-end.

**Don't be scared by the number of packages**. Most of the packages are very
small, and you don't have to interact with all of them at once. All Go language
files under the `trib` directory are less than 2500 lines in total (the beauty
of Go!), so these packages aren't huge and intimidating.

Through the entire lab, you do not need to (and should not) modify anything
in the `trib` repository. If you feel that you have to change some
code to complete your lab, please first discuss it with the TA. You are always
welcome to read the code in the `trib` repository. If you find a bug and report
it, you might get some bonus credit.

## Your Job

Your job is to complete the implementation of the `triblab`
package.  It is in the second repo that we checked out.

It would be good practice for you to periodically commit your code into your
`triblab` git repo. **Only commited files in that `triblab` will be submitted
for grading**, so even if you aren't using the git repository for your own
version control (pro tip: use it), you will need to commit all of your files at
least once right before before turning in. If you've never used git before, make
sure you understand what this means before trying to submit your code.

## Lab Roadmap

- **Lab 1**. Wrap the key-value storage service with RPC so that a remote
  client can store data remotely.
- **Lab 2**. Reimplement the Tribbler service, splitting the current
  Tribbler logic into stateless scalable front-ends and scalable key-value store
  back-ends. The front-ends will call the back-ends via the RPC mechanism
  implemented in Lab 1. When this lab is done, you will have made both the
  front-end and the back-end scalable.
- **Lab 3**. We make the back-ends fault-tolerent with replication
  and by using techniques like distributed hash tables. At the end of this lab,
  back-end servers can join, leave, or be killed, without affecting the service.

By the end of the labs, you will have an implementation of Tribbler that
is scalable and fault-tolerant.

## Misc

Go has expectations about environment variables. For convenience, you might
set these variables in your `.bashrc` and/or
`.bash_profile` files so that you don't have to execute the commands
every time:

```
export GOPATH=$HOME/gopath
export PATH=$PATH:$GOPATH/bin
```

We should have Vim and Emacs installed on the machines. If you need
to install other utility packages, ask the TA. Note that you do not
have `sudo` permissions on any of the machines; any `sudo` attempt
will be automatically reported, so please don't even try it.

You could also write your code on your own machine if you want to.
See Go language's [install](http://golang.org/doc/install) page for
more information. However, you should test and submit your code on the lab
machines.

## Ready?

If you feel comfortable with the lab setup, continue on to [Lab1](./lab1.html).
