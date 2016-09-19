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

## License

Copyright © 2016 Kevin Lynagh & Sebastian Jylanki

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
