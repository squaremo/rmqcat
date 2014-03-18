rmqcat
======

A netcat-like tool for sending things through RabbitMQ.

Installation:

```sh
npm install -g rmqcat
rmqcat --help
```

Use:

```sh
# Start a listener that will put whatever it gets in a file
rmqcat -l > recv.txt

# Send a file to the listener
rmqcat < send.txt
```

In general, `rmqcat` will keep a connection open until it gets
end-of-file, so you can use it to "chat" back and forth, similar to
netcat.

A client (i.e., without `-l`) will buffer input until its connection
is accepted by a listener.

The option `-k` in combination with `-l` will keep the listener
accepting successive connections. Otherwise it will close once the
first connection closes.

The options `--url` and `--service` can be used to address a RabbitMQ
server elsewhere (url) with a particular queue used for opening
connections (service). By default a RabbitMQ server on localhost is
assumed, so you will probably want to at least supply `--url` in
practice. The service name defaults to `"rmqcat"`.

The option `-D` will make rmqcat output a bit of debug information to
stderr.

The option `--help`, if present at all, will make rmqcat output a
usage message to stderr then exit.
