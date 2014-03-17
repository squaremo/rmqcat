var amqp = require('amqplib');
var Readable = require('stream').Readable
  || require('readable-stream/readable');
var Writable = require('stream').Writable
  || require('readable-stream/writable');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;

var options = require('yargs')
  .example('$0 < foobar.txt', 'Send a file')
  .example('$0 -lk', 'Listen for connections and output data to stdout')

  .options('url', {
    'default': 'amqp://localhost',
    describe:'Connect to the RabbitMQ at <url>'})
  .options('queue', {
    'default': 'rmqcat',
    describe: 'Use the service at <queue>'})
  .options('help', {
    describe: 'Print help'})

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

var handshakeQ = argv.queue;

var ok = amqp.connect(url);
ok.then(function(connection) {

  return connection.createChannel().then(function(ch) {

    // It's convenient, since it's passed around, to use the channel
    // closing a a signal to clean up and leave.
    ch.on('close', function() {
      connection.close().then(function() {
        process.exit(0);
      });
    });

    // Always sure the handshake queue exists, since we don't know who
    // will turn up first
    debug('Asserting handshake queue: %s', handshakeQ);
    ch.assertQueue(handshakeQ);

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
            process.stdin.unpipe();
            acceptCh.ack(accepted);
          }

          acceptCh.prefetch(1);
          // Any returned messages are a result of the 'open' not
          // being routed; this is interpreted as the connection
          // having disappeared (possibly before we even turned up)
          acceptCh.on('return', function(returned) {
            debug('Open message returned form %s',
                  returned.fields.routingKey);
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
              process.stdin.pipe(current, {end: true});
              break;
            default:
              console.warn('Something other than open, %s ',
                           msg.properties.type,
                           'received on handshake queue');
            }
          }, {exclusive: true});

          var streams = new QueueStreamServer(ch, stdinQ);
          streams.on('connection', function(stream) {
            stream.pipe(process.stdout, {end: !argv.k});
            stream.on('end', function() {
              current.end();
            });
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
            var relay = writableQueue(ch, stdinQ);
            process.stdin.pipe(relay, {end: true});
            break;
          case 'data':
            debug('Recv %d bytes on stdout', msg.content.length);
            process.stdout.write(msg.content);
            break;
          case 'eof':
            debug('Recv eof on stdout (%s)', stdoutQ);
            ch.close();
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
        self.emit('error', new Error('Input queue deleted'))}); // fall-through
    case 'eof':
      debug('Recv eof on %s', queue);
      current.push(null);
      current = null;
      break;
    case 'data':
      debug('Recv %d bytes on %s', msg.content.length, queue);
      current.push(msg.content); break;
    default:
      console.warn('Unknown message type %s', msg.properties.type);
    }
  }, {exclusive: true, noAck: true});
}
inherits(QueueStreamServer, EventEmitter);
