var amqp = require('amqplib');

var argv = require('yargs')
  .default('url', 'amqp://localhost')
  .default('queue', 'rmqcat')
  .boolean('l')
  .argv;

var url = argv.url;
var queue = argv.queue;

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

    if (argv.l) {
      ch.assertQueue(queue);
      ch.consume(queue, function(msg) {
        switch (msg.properties.type) {
        case 'open':
          // %%% The idea is to switch relaying input to the replyTo
          break;
        case 'stdin':
          process.stdout.write(msg.content); break;
        case 'eof':
          if (!argv.k) ch.close(); break;
        default:
          // um
        }
      }, {noAck: true, exclusive: true});
      startRelay(channel, replyTo, queue);
    }

    else {
      ch.assertQueue(queue);
      ch.assertQueue().then(function(ok) {
        var replyQ = ok.queue;

        ch.consume(replyQ, function(msg) {
          switch (msg.properties.type) {
          case 'stdin':
            process.stdout.write(msg.content); break;
          case 'eof':
            process.stdout.end();
            ch.close();
            break;
          default:
            // um
          }
        }, {noAck: true, exclusive: true});

        ch.sendToQueue(queue, new Buffer(0), {type: 'open'});
        startRelay(ch, queue, replyQ);
      });
    }
  });
}, console.warn);

function startRelay(channel, queue, replyTo) {
  relay(process.stdin, queue, channel, 'stdin', replyTo);
  process.stdin.on('end', function() {
    channel.sendToQueue(queue, new Buffer(0), {'type': 'eof'});
    channel.close();
  });
}

function relay(stream, queue, channel, label, replyTo) {
  function go() {
    var b; while (b = stream.read()) {
      channel.sendToQueue(queue, b, {'type': label, replyTo: replyTo});
    }
  }
  stream.on('readable', go);
}
