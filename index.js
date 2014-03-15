var amqp = require('amqplib');

var url = 'amqp://localhost';
var queue = process.argv[2] || 'rmqcat';

var ok = amqp.connect(url);
ok.then(function(connection) {

  return connection.createChannel().then(function(ch) {

    process.stdin.on('end', function() {
      ch.sendToQueue(queue, new Buffer(0), {'type': 'eof'});
      ch.close().then(function() {
        connection.close();
      });
    });

    ch.assertQueue(queue);
    startRelays(ch, queue);
  });

}, console.warn);

function startRelays(channel, queue) {
  relay(process.stdin, queue, channel, 'stdin');
}

function relay(stream, queue, channel, label) {
  function go() {
    var b; while (b = stream.read()) {
      channel.sendToQueue(queue, b, {'type': label});
    }
  }

  stream.on('readable', go);
}
