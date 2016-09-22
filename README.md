# zmqchans

Control ZMQ Sockets via core.async interface.

This project is a deadlock free alteration of Kevin Lynagh's
zmq-async library (https://github.com/lynaghk/zmq-async).

### *Documentation under construction*

## Usage

```clojure
(require '[zmqchans.core :refer [socket]])
(require '[clojure.core.async :refer [>!! <!!]])

(let [rep (socket :rep :bind "inproc://testing")
      req (socket :req :connect "inproc://testing")]
  (>!! (req :in) ["foo" "bar" (.getBytes "baz")])
  (<!! (rep :out))
)
```

## Known issues

### JVM crashing when terminating contexts

#### Problematic configurations

- Linux 3.13, libzmq 4.1.4, jzmq Snapshot 3.1.1-SNAPSHOT (commit: 8922cd2)

#### Description

This is a lower level issue either caused by jzmq or libzmq. It happens very randomly with with inproc and ipc transports. Not sure if it may happen with tcp also.

#### Solution

Be mindful when terminating contexts. JVM might crash. To avoid this issue do not actually close the context, just close/remove sockets from it. You can achieve this by setting `.setMain` to `false` on `ZContext`. This option defaults to true. See the following code snippet:

```clojure
(let [ctx (context)] (.setMain (ctx :zcontext) false) ...)
```

The downside to this is that it will leave one or more idle threads controlled by libzmq running, consuming a little bit of resources. It shouldn't really be an issue unless you leave a lot of contexts in this state.

#### Related links

* https://github.com/zeromq/libzmq/issues/2117



### Sockets using inproc transport randomly not initiating a connection

#### Problematic configurations

- Linux 3.13, libzmq 4.1.4, jzmq Snapshot 3.1.1-SNAPSHOT (commit: 8922cd2)

#### Description

Sometimes when creating socket pair (especially `:rep` / `:req`) connected to each other, the sockets won't just communicate. When you send a message to `:in` the message gets delivered normally inside zmqasync internally but `zmq_poll` never gets a signal. I haven't investigated whether there is actually data in the socket or not. It's a low level issue caused by libzmq or jzmq.

Once the connection has been established and data is flowing through it seems that everything will keep on working after that point.

This is really quite a bad bug that makes me hesitant to use `inproc` transport (especially `:rep` / `:req`) on any production system. zmqasync is using ipc instead of inproc internally to avoid this problem. More testing is required to see if this is only occurring with `:rep` / `:req` pair.

#### Solution

If you plan on using inproc transport, make sure to test your system very thoroughly for deadlocks. I recommend using ipc for all but the most performance sensitive tasks. You can always just use core.async channels for intra-process communication anyway to avoid this problem.

#### Future plans

Test more to see whether inproc is safe to use with other than `:rep` / `:req` pairs.

## Thanks

Thanks to Kevin Lynagh (@lynaghk) for the initial idea and implementation
(https://github.com/lynaghk/zmq-async). Thanks to those who helped him during that process,
mentioned in https://github.com/lynaghk/zmq-async#thanks.

## License

Copyright © Sebastian Jylanki. Some functions directly copied from Kevin Lynagh's zmq-async
library.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
