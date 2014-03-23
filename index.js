#!/usr/bin/env node
var amqp = require('amqplib');
var Readable = require('stream').Readable
  || require('readable-stream/readable');
var Writable = require('stream').Writable
  || require('readable-stream/writable');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var spawn = require('child_process').spawn;

var options = require('yargs')
  .example('$0 < foobar.txt', 'Send a file')
  .example('$0 -lk', 'Listen for connections and output data to stdout')

  .options('url', {
    'default': 'amqp://localhost',
    describe:'Connect to the RabbitMQ at <url>'})
  .options('service', {
    'default': 'rmqcat',
    describe: 'Use the service at <queue>'})
  .options('help', {
    describe: 'Print help and exit'})

  .options('send', {
    describe: 'Send directly to <queue>'})
  .options('recv', {
    describe: 'Receive directly from <queue>'})

  .options('exec', {
    describe: 'Spawn a process and use stdin and stdout from that process',
    alias: 'e'
  })

  .describe('l', 'Listen for connections')
  .describe('k', 'Keep listening after client disconnections')
  .describe('D', 'Output debug information to stderr')
  .boolean(['l', 'k', 'D']);

var argv = options.argv;

if (argv.help) {
  options.showHelp();
  process.exit(0);
}

var debug = (argv.D) ? console.warn : function() {};

var url = argv.url;

// I use three different (kinds of) queues: there is the handshake
// queue, which is the common knowledge between the client and the
// server (the "connection point", like a port); then is a stdin queue
// and a stdout queue, both named from the point of view of the
// server.

// The stdin queue is that over which the client sends data to the
// server, and the stdout queue is that over which the server sends
// data to the client. The client creates the stdin queue and annouces
// it to the server (as the 'replyTo' of an empty message sent to the
// handshake queue); the server creates the stdout queue and announces
// it to the client (likewise, sent to the stdout queue).

var handshakeQ = argv.service;

var ok = amqp.connect(url);
ok.then(function(connection) {

  return connection.createChannel().then(function(ch) {

    // It's convenient, since most of the work goes through the
    // channel, to use its closure as a signal to clean up and leave.
    ch.on('close', function() {
      connection.close().then(function() {
        process.exit(0);
      });
    });

    // send and recv don't use a service (handshake) queue, they just
    // send to or receive from the queue mentioned.
    if (argv.send || argv.recv) {

      if (argv.send) {
        var dest = argv.send;
        ch.assertQueue(dest);
        var out = writableQueue(ch, dest);
        process.stdin.pipe(out);
        out.on('finish', function() {
          ch.close();
        });
      }

      else if (argv.recv) {
        var source = argv.recv;
        ch.assertQueue(source);
        var reader = readableQueue(ch, source);
        reader.on('end', function() {
          ch.close();
        });
        reader.pipe(process.stdout);
      }

      return; // no more options matter
    }

    // Make sure the handshake queue exists, since we don't know who
    // will turn up first
    debug('Asserting handshake queue: %s', handshakeQ);
    ch.assertQueue(handshakeQ);

    var stdin;
    var stdout;

    function setup() {
      if (argv.e) {
        debug('Starting process %s', argv.e);
        var args = argv.e.split(' ');
        var child = spawn(args[0], args.slice(1));
        stdin = child.stdout;
        stdout = child.stdin;
      }
      else {
        stdin = process.stdin;
        stdout = process.stdout;
      }
    }

    if (argv.l) { // act as server
      ch.assertQueue('', {exclusive: true}).then(function(ok) {
        var stdinQ = ok.queue;
        debug('Created stdin queue: %s', stdinQ);

        // I need a channel on which to accept connections. Why
        // another? Because this one only deals with one connection at
        // a time (so it can rewire stdin and stdout appropriately),
        // and to do that, I must use prefetch=1 so I don't get sent
        // all connection requests at once.
        connection.createChannel().then(function(acceptCh) {
          var accepted = null;
          var current = null;

          function next() {
            stdin.unpipe();
            acceptCh.ack(accepted);
          }

          acceptCh.prefetch(1);
          // Any returned messages are a result of the 'open' not
          // being routed; this is interpreted as the connection
          // having disappeared (possibly before we even turned up)
          acceptCh.on('return', function(returned) {
            debug('open message returned from %s',
                  returned.fields.routingKey,
                  ' assuming dead connection');
            next();
          });

          acceptCh.consume(handshakeQ, function(msg) {
            switch (msg.properties.type) {
            case 'open':
              accepted = msg;
              var stdoutQ = msg.properties.replyTo;
              debug('Recv open: stdout is %s', stdoutQ);
              acceptCh.sendToQueue(stdoutQ, new Buffer(0),
                                   {type: 'open',
                                    mandatory: true,
                                    replyTo: stdinQ});
              debug('Sent open to %s: stdin is %s', stdoutQ, stdinQ);
              current = writableQueue(ch, stdoutQ);
              current.on('finish', function() {
                next();
                if (!argv.k) {
                  ch.close();
                }
              });
              setup();
              stdin.pipe(current, {end: true});
              break;
            default:
              console.warn('Something other than open, %s ',
                           msg.properties.type,
                           'received on handshake queue');
            }
          }, {noAck: false});

          var streams = new QueueStreamServer(ch, stdinQ);
          streams.on('connection', function(stream) {
            stream.pipe(stdout, {end: !argv.k});
            //stream.on('end', function() {
            //  current.end();
            //});
          });
        });
      });
    }

    else { // act as client
      ch.assertQueue('', {exclusive: true}).then(function(ok) {
        var stdoutQ = ok.queue;
        debug('Created stdout queue %s', stdoutQ);

        ch.consume(stdoutQ, function(msg) {
          switch (msg.properties.type) {
          case 'open':
            var stdinQ = msg.properties.replyTo;
            debug('Recv open: stdin is %s', stdinQ);
            setup();
            var relay = writableQueue(ch, stdinQ);
            stdin.pipe(relay, {end: true});
            break;
          case 'data':
            debug('Recv %d bytes on stdout', msg.content.length);
            stdout.write(msg.content);
            break;
          case 'eof':
            debug('Recv eof on stdout (%s)', stdoutQ);
            if (stdout !== process.stdout) stdout.end();
            break;
          default:
            console.warn('Unknown message type %s',
                         msg.properties.type,
                         ' received on stdout queue');
          }
        }, {noAck: true, exclusive: true});

        ch.sendToQueue(handshakeQ, new Buffer(0),
                       {type: 'open', replyTo: stdoutQ});
        debug('Sent open to handshake queue %s', handshakeQ);
      });
    }
  });
}, console.warn);

// Create a writable stream that sends chunks to a queue.
function writableQueue(channel, queue) {
  var writable = new Writable();
  writable._write = function(chunk, _enc, cb) {
    debug('Sent %d bytes to %s', chunk.length, queue);
    if (channel.sendToQueue(queue, chunk, {type: 'data'})) {
      cb();
    }
    else channel.once('drain', cb);
  };
  writable.on('finish', function() {
    channel.sendToQueue(queue, new Buffer(0), {type: 'eof'});
    debug('Sent eof to %s', queue);
  });
  return writable;
}

// Create a readable stream that gets chunks from a stream.
function readableQueue(channel, queue) {
  var readable = new Readable();
  readable._read = function() {};

  // Logically, we want to receive everything up to EOF and no
  // more. However, in practice there's no way to switch the tap off
  // at an exact message; instead, we will overrun slightly, so we
  // need to put those extra messages back in the queue by nacking
  // them.
  var running = true;

  var ok = channel.consume(queue, function(msg) {
    if (!running) {
      channel.nack(msg);
      return;
    }

    switch (msg && msg.properties.type) {
    case null: // cancelled by server
      readable.emit('error', new Error('Consume cancelled by server'));
      break;
    case 'eof':
      running = false;
      // Don't trigger anything (e.g., closing the channel) until
      // we've cancelled. We may get messages in the meantime, which
      // is why the nack and early return above.
      ok.then(function(consumeOk) {
        channel.cancel(consumeOk.consumerTag);
        readable.push(null);
      });
      break;
    case 'data':
      readable.push(msg.content);
      break;
    default:
      console.warn('Unknown message type %s', msg.properties.type);
    }
    channel.ack(msg);
  }, {exclusive: true, noAck: false});

  return readable;
}

function QueueStreamServer(channel, queue) {
  EventEmitter.call(this);

  var self = this;
  var current = null;

  channel.consume(queue, function(msg) {
    if (current === null) {
      current = new Readable();
      current._read = function() {};
      self.emit('connection', current);
    }

    switch (msg && msg.properties.type) {
    case null: // consume has been cancelled
      debug('Consume cancelled (%s)', queue);
      setImmediate(function() {
        self.emit('error', new Error('Input queue deleted'))});
      // fall-through
    case 'eof':
      debug('Recv eof on %s', queue);
      current.push(null);
      current = null;
      break;
    case 'data':
      debug('Recv %d bytes on %s', msg.content.length, queue);
      current.push(msg.content);
      break;
    default:
      console.warn('Unknown message type %s', msg.properties.type);
    }
  }, {exclusive: true, noAck: true});
}
inherits(QueueStreamServer, EventEmitter);
