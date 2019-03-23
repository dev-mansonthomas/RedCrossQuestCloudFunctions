const Firestore    = require('@google-cloud/firestore');


/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 * Update an existing TroncQueteur with new data
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.notifyRedQuestOfRegistrationApproval = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Received Message for notifyRedQuestOfRegistrationApproval function : "+JSON.stringify(parsedObject));



};
