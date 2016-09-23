# zmqchans

Control ZMQ Sockets via core.async interface.

This project is a deadlock free alteration of Kevin Lynagh's zmq-async library (https://github.com/lynaghk/zmq-async).

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

## Thanks

Thanks to Kevin Lynagh (@lynaghk) for the initial idea and implementation (https://github.com/lynaghk/zmq-async). Thanks to those who helped him during that process, mentioned in https://github.com/lynaghk/zmq-async#thanks.

## License

Copyright © Sebastian Jylanki. Some functionality copied from Kevin Lynagh's zmq-async library with or without modifications.

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
