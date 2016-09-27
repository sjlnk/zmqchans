## TODO:

### Include support for ZMQ_DGRAM sockets

## Possible future improvement ideas:

### Add support for generic file descriptors

JZMQ does not support this at the moment and a support should be added to that library first. zmq_poll has support for arbitrary file descriptors. This functionality will not work with MS Windows, afaik. If there is high enough demand from community to add such functionality I'll think about it more.
