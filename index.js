const kafka = require('kafka-node');

const ipAddress = '192.168.77.100:2181';
const client = new kafka.Client(ipAddress);

const producer = new kafka.Producer(client);

const topicInput = 'streams-file-input';
const topicOutput = 'streams-file-output';

const consumer = new kafka.Consumer(client, [
    { topic: 'streams-file-input' }
  ], { autoCommit: false });

function processWordCount(line) {
  return line.split(' ')
                .map((v) => { let o = {}; o[v] = 1; return o; })
                .reduce((a,b) => {
                  if (!Object.keys(a).includes(Object.keys(b)[0])) return Object.assign(a, b);
                  let c = Object.assign({}, a);
                  c[Object.keys(b)] = a[Object.keys(b)[0]] + 1;
                  return c;
                } );
}

producer.on('ready', function () {
  consumer.on('message', (textLine) => {
    producer.send([{ topic: topicOutput, messages: JSON.stringify(processWordCount(textLine.value)) }], (err, data) => {
      if (err) throw err;
      // console.log('kafka downstreamed:', data);
    })
  });
});


setInterval(() => { console.log('tick'); }, 1000);