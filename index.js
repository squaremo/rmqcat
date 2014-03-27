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
// server (the "connection point", like a port); then there is an in
// queue and an out queue, both named from the point of view of the
// process.

// The stdin queue is that over which the remote sends data to the
// this process, and the out queue is that over which the process
// sends data to the remote. This process creates the in queue and
// annouces it to the remote (as the 'replyTo' of an empty message);
// the remote creates the out queue and announces it to this process.

// The difference between a client and a server (listener) is that the
// client sends an open message to the handshake queue, and the
// listener responds with an open message to the client's in queue
// (which becomes the server's out queue).

var handshakeQ = argv.service;


function closeLatch(done) {

  function either(which) {
    switch (which) {
    case 'in':
      return only('out');
    case 'out':
      return only('in');
    default:
      throw new Error('Unknown stream ' + which);
    }
  }
  function only(s) {
    return function(which) {
      switch (which) {
      case s:
        return done();
      default:
        throw new Error('Close on stream other than expected ' + s);
      }
    }
  }
  return either;
}

function neither(which) {
  throw new Error('Attempted to close ' + which + '; both already closed');
}


var ok = amqp.connect(url);
ok.then(function(connection) {

  return connection.createChannel().then(function(ch) {

    // Simplex

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

        process.on('SIGINT', function() {
          process.stdin.unpipe();
          out.end();
        });

        out.on('finish', function() {
          ch.close();
        });
      }

      else if (argv.recv) {
        var source = argv.recv;
        ch.assertQueue(source);
        var reader = readableQueue(ch, source);

        var torndown = false;

        function teardown() {
          if (torndown) return;
          torndown = true;
          debug("Tearing down pipe to stdout");
          reader.stop();
          reader.unpipe();
          ch.close();
        }

        process.on('SIGINT', teardown);
        // If we're being piped into another process, and that process
        // terminates or otherwise closes its input, we can get an
        // EPIPE exception here, possibly more than once.
        process.stdout.on('error', function(err) {
          if (err.code === 'EPIPE') {
            debug(err);
            teardown(); }
          else
            throw err;
        });

        reader.on('end', function() {
          ch.close();
        });
        reader.pipe(process.stdout);
      }

      return; // no more options matter
    }

    // Duplex

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
        stdin.on('end', function() {
          debug('Child process output ended');
        });
        stdout = child.stdin;
      }
      else {
        stdin = process.stdin;
        stdout = process.stdout;
      }
    }

    if (argv.l) { // act as server
      return ch.assertQueue('', {exclusive: true}).then(function(ok) {
        var inQ = ok.queue;
        debug('Created in queue: %s', inQ);

        // I need a channel on which to accept connections. Why
        // another? Because this one only deals with one connection at
        // a time (so it can rewire stdin and stdout appropriately),
        // and to do that, I must use prefetch=1 so I don't get sent
        // all connection requests at once.
        connection.createChannel().then(function(acceptCh) {
          var accepted = null;
          var writable = null;

          function next() {
            stdin.unpipe();
            acceptCh.ack(accepted);
          }

          var latch;
          if (argv.k) {
            var freshLatch = closeLatch(function() {
              next();
              return freshLatch;
            });
            latch = freshLatch;
          }
          else {
            latch = closeLatch(function() {
              next();
              ch.close();
              return neither;
            });
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
              var outQ = msg.properties.replyTo;
              debug('Recv open: out queue is %s', outQ);
              acceptCh.sendToQueue(outQ, new Buffer(0),
                                   {type: 'open',
                                    mandatory: true,
                                    replyTo: inQ});
              debug('Sent open to out queue %s: in queue is %s', outQ, inQ);
              writable = writableQueue(ch, outQ);
              writable.on('finish', function() {
                latch = latch('out');
              });
              setup();
              stdin.pipe(writable, {end: true});
              break;
            default:
              console.warn('Something other than open, %s ',
                           msg.properties.type,
                           'received on handshake queue');
            }
          }, {noAck: false});

          var streams = new QueueStreamServer(ch, inQ);
          streams.on('connection', function(readable) {
            // process.stdout doesn't like to have `#end` called on
            // it'; however, pipe appears to know not to do so, and I
            // *do* want it called if stdout is the input to an
            // `--exec`.
            readable.pipe(stdout, {end: true});
            readable.on('end', function() {
              latch = latch('in');
            });

            // The special case for closing client streams: if we're
            // accepting input on stdin, treat the server closing as us
            // closing.
            if (stdin === process.stdin) {
              readable.on('end', function() {
                writable.end();
              });
            }
          });
        });
      });
    }

    else { // act as client
      var latch = closeLatch(function() {
        ch.close();
        return neither;
      });

      ch.assertQueue('', {exclusive: true}).then(function(ok) {
        var outQ = ok.queue;
        debug('Created out queue %s', outQ);

        var readable = readableQueue(ch, outQ, function(inQ) {
          setup();
          readable.pipe(stdout);
          var writable = writableQueue(ch, inQ);
          stdin.pipe(writable, {end: true});
          writable.on('finish', function() {
            latch = latch('out');
          });

          // The special case for closing client streams: if we're
          // accepting input on stdin, treat the server closing as us
          // closing.
          if (stdin === process.stdin) {
            readable.on('end', function() {
              writable.end();
            });
          }
        });
        readable.on('end', function() {
          latch = latch('in');
        });

        ch.sendToQueue(handshakeQ, new Buffer(0),
                       {type: 'open', replyTo: outQ});
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
function readableQueue(channel, queue, openCb) {
  var readable = new Readable();
  readable._read = function() {};

  // Logically, we want to receive everything up to EOF and no
  // more. However, in practice there's no way to switch the tap off
  // at an exact message; instead, we will overrun slightly, so we
  // need to put those extra messages back in the queue by nacking
  // them.
  var running = true;

  // Don't trigger anything (e.g., closing the channel) until
  // we've cancelled. We may get messages in the meantime, which
  // is why the nack and early return above.
  function stop() {
    running = false;
    ok.then(function(consumeOk) {
      channel.cancel(consumeOk.consumerTag);
      readable.push(null);
    });
  }

  readable.stop = stop;

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
      stop();
      break;
    case 'data':
      debug('Recv %d bytes', msg.content.length);
      readable.push(msg.content);
      break;
    case 'open':
      var inQ = msg.properties.replyTo;
      debug('Recv open: in queue is %s', inQ);
      openCb(inQ);
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
        self.emit('error', new Error('In queue deleted'))});
      // fall-through
    case 'eof':
      debug('Recv eof on in queue %s', queue);
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
