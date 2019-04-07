const {BigQuery}   = require('@google-cloud/bigquery');
const {PubSub}     = require('@google-cloud/pubsub');

const topicName    = 'queteur_data_updated';
const BQdataSet    = 'redcrossquest';
const BQTableName  = 'tronc_queteur';

const bigquery     = new BigQuery();
const pubsubClient = new PubSub();

const troncQueteurTable =  bigquery
  .dataset( BQdataSet   )
  .table  ( BQTableName );

const topicPublisher =  pubsubClient
  .topic     (topicName)
  .publisher ();

function handleError(err){
  if (err && err.name === 'PartialFailureError') {
    if (err.errors && err.errors.length > 0) {
      console.log('Insert errors:');
      err.errors.forEach(err => console.error(err));
    }
  } else {
    console.error('ERROR:', err);
  }
}


/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.processNewTroncQueteur = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Recieved Message : "+JSON.stringify(parsedObject));

  const queteur = { queteur_id: parsedObject.queteur_id, ul_id:parsedObject.ul_id };

  troncQueteurTable
    .insert (parsedObject, {'ignoreUnknownValues':true, 'raw':false})
    .then   ((data) => {
      console.log(`Inserted 1 rows` + JSON.stringify(data));

      const dataToPublish = JSON.stringify(queteur);
      const dataBuffer    = Buffer.from(dataToPublish);

      topicPublisher
        .publish   (dataBuffer)
        .then      ((data)=>{
          console.log(`Published 1 message `+JSON.stringify({ queteur_id: parsedObject.queteur_id, data:data }));
        })
        .catch(err=>{
          handleError(err);
        });
    })
    .catch(err => {
      handleError(err);
    });
};
