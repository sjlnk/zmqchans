# zmqchans

Control ZMQ Sockets via core.async interface.

This project is a deadlock free alteration of Kevin Lynagh's
zmq-async library (https://github.com/lynaghk/zmq-async).

Documentation under construction. 

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

## Troubleshooting

### Assertion failed: written (/home/seb/src/libzmq/src/ctx.cpp:583) or something similar when shutting down context:

This is a bug in libzmq, see https://github.com/zeromq/libzmq/issues/2117

### Sockets using inproc transport randomly not delivering messages.

This is really bad, will cause random malfunctions because zmqasync internally communicates via
inproc sockets. Try updating libzmq. This problem occurred to me with libzmq 4.0.4. It may have
been also due to version missmatch between jzmq and libzmq.

## License

Copyright © 2016 Kevin Lynagh & Sebastian Jylanki

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
