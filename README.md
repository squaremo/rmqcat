rmqcat
======

A netcat-like tool for sending things through RabbitMQ.

## Installation

```sh
npm install -g rmqcat
rmqcat --help
```

## Use

`rmqcat` has two modes of use, one-way and two-way ("duplex"). Duplex
corresponds more or less to how `netcat` works, that is, it
establishes a socket-like connection with a server ('listener') on one
side and a client on the other, which can speak back and forth.

One-way (simplex) either relays stdin to a RabbitMQ queue, *or* from a
RabbitMQ queue to stdout. Sending to a queue doesn't wait for a
receiver; receiving from a queue waits for data in the queue.

### Common to both modes

The option `--url` can be used to address a specific RabbitMQ server,
and to provide connection parameters -- see the [amqplib
documentation][amqplib-doc-url].  By default a RabbitMQ server on
localhost is assumed, so you will probably want to supply `--url` in
practice.

The option `-D` will make rmqcat output a bit of debug information to
stderr.

The option `--help`, if present at all, will make rmqcat output a
usage message to stderr then exit.

### Duplex

```sh
# Start a listener that will put whatever it gets in a file
rmqcat -l > recv.txt

# Send a file to the listener
rmqcat < send.txt
```

`rmqcat` used this way will keep a connection open until it gets
end-of-file, so you can use it to "chat" back and forth, similar to
netcat.

A client (i.e., without `-l`) will buffer input until its connection
is accepted by a listener.

The option `-k` in combination with `-l` will keep the listener
accepting successive connections. Otherwise it will exit once the
first connection closes.

The option `--service` has a role similar to a TCP port number. It
names a queue to be used by clients and listeners to establish
connections. The default is arbitrarily `"rmqcat"`.

### One-way

```sh
# Send a file to a queue
rmqcat --send bobbins < bobbins.iso

# Save the file in a queue and output the SHA1 sum
rmqcat --recv bobbins | tee bobbins.iso | shasum

The string following either `--send` or `--recv` names a queue that
will hold the data in transit. More than one file of data can be
present in the queue; `rmqcat --recv <queue>` will read a single file
before exiting, or wait if there is no data yet.

[amqplib-doc-url]: http://squaremo.github.io/amqp.node/doc/channel_api.html
