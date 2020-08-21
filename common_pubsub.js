'use strict';
const {PubSub}        = require('@google-cloud/pubsub');
const pubsubClient    = new PubSub();

async function publishMessage(topicName, data)
{
  const dataBuffer  = Buffer.from(JSON.stringify(data));

  await pubsubClient
    .topic     (topicName)
    .publish   (dataBuffer);
}

module.exports = {
  publishMessage : publishMessage,
  pubsubClient:pubsubClient
};
