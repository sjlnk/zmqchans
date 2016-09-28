# zmqchans

Thread-safe Clojure core.async interface to ZeroMQ

`zmqchans` lets you work with ZeroMQ sockets using `core.async` channels in a thread-safe manner.

ZeroMQ is a great networking/IPC library and `core.async` is a great inprocess communication library. Unfortunately they don't work together very well without some glue code. Without zmqchans or alternatives you have to create ad hoc solutions for every project dealing with the separate communication with ZMQ sockets. They cannot easily be added to you main socket polling loop, you need a separate thread of execution for that. With zmqchans all of this can be achieved in one or two lines of code and then you have ZMQ sockets right there on your clojure.core.async toolbox.

There are some other open source projects that have provided their own solutions for this problem. Especially Kevin Lynagh's `zmq-async` library has been a great reference and source of inspiration when creating this library. Due to an architectural oversight it's easy to create deadlocks in that library. That was a reason why an improved design was required.

Improvements over `zmq-async`:

* Deadlock free design: no two-way communication between injection thread and ZMQ socket polling thread.
* Easy to use API with minimal boilerplate.
* Full thread-safe access to the sockets that have been added to the ZMQ polling thread, not only message delivery.

## Example

```clojure
(let [pub (socket :pub "@tcp://*:*")
      sub (doto (socket :sub) (connect! pub) (subscribe! ""))]
  (send! pub "broadcast")
  (String. (recv! sub)))
```

## Documentation

Please refer to wiki section.

## Support

You can send email directly to sebastian.jylanki@gmail.com if you need assistance. If you have feature ideas or find some bugs please use the github's issue feature.

## Thanks

Thanks to Kevin Lynagh (@lynaghk) for the initial idea and implementation (https://github.com/lynaghk/zmq-async). Thanks to those who helped him during that process, mentioned in https://github.com/lynaghk/zmq-async#thanks.

## License

Copyright © Sebastian Jylanki. Some functionality copied from Kevin Lynagh's zmq-async library with or without modifications.

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
