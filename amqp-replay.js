'use strict';

const _ = require('lodash');
const readline = require('readline');
const util = require('util');
const logger = require('winston');
const when = require('when');
logger.setLevels(logger.config.syslog.levels);
const env = require('common-env/withLogger')(logger);
const config = env.getOrElseAll({
  amqp: {
    uri: 'amqp://guest:guest@localhost:5672/%2F',
    queue: {
      name: 'dead-letter-queue',
      noAck: false
    }
  }
});

const rl = readline.createInterface({input: process.stdin, output: process.stdout});

require('amqplib').connect(config.amqp.uri).then(function(conn) {
  const exit = () => conn.close();
  process.once('SIGINT', exit);

  const queueCh = conn.createChannel();
  const exchangeCh = conn.createChannel();

  const prefetch = queueCh.then(ch => ch.prefetch(1));
  const queueOk = queueCh.then(ch => ch.checkQueue(config.amqp.queue.name));
//  const exchangeOk = exchangeCh.then(ch => ch.checkExchange(config.amqp.exchange.name));

  when.join(queueCh, queueOk, exchangeCh, prefetch)
    .spread(function(queueCh, queueOk, exchangeCh) {
      return queueCh.consume(config.amqp.queue.name, function(msg) {
        const fields = msg.fields;
        const properties = msg.properties;
        const content = msg.content;

        const replayExchange = _.get(properties, 'headers["x-death"][0].exchange');
        const replayKey = _.get(properties, 'headers["x-death"][0]["routing-keys"][0]');

        if (!replayKey || !replayExchange) {
          throw new Error('Could not determine routing key for message ' + fields);
        }
   
        const ques = util.format('About to replay message %j with key %s to %s, do you want to continue?', fields, replayKey, replayExchange);
        rl.question(ques, answer => {
          if (answer !== 'y') {
            logger.info('Skipping replay and exiting');
            rl.close();
            exit();
            return;
          }
          if(exchangeCh.publish(replayExchange, replayKey, content, properties)){
            queueCh.ack(msg);
          }

        });
      }, {
        noAck: config.amqp.noAck
      });
    })
    .then(_consumeOk => logger.debug('Waiting for messages. To exit press CTRL+C'))
    .otherwise(err => {
      logger.error('error', err);
      exit();
    });
}).then(null, console.warn);
