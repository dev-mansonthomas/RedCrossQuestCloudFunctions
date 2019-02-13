const {BigQuery}   = require('@google-cloud/bigquery');
const {PubSub}     = require('@google-cloud/pubsub');

const topicName    = 'queteur_data_updated';

const bigquery     = new BigQuery();
const pubsubClient = new PubSub();

const topicPublisher =  pubsubClient
  .topic     (topicName)
  .publisher ();

const queryStr = [
  'update `redcrossquest.tronc_queteur` as tq         ',
  'set                                                ',
  '      tq.point_quete_id      = :point_quete_id     ',
  '      tq.depart_theorique    = :depart_theorique   ',
  '      tq.depart              = :depart             ',
  '      tq.retour              = :retour             ',
  '      tq.comptage            = :comptage           ',
  '      tq.last_update         = :last_update        ',
  '      tq.last_update_user_id = :last_update_user_id',
  '      tq.euro500             = :euro500            ',
  '      tq.euro200             = :euro200            ',
  '      tq.euro100             = :euro100            ',
  '      tq.euro50              = :euro50             ',
  '      tq.euro20              = :euro20             ',
  '      tq.euro10              = :euro10             ',
  '      tq.euro5               = :euro5              ',
  '      tq.euro2               = :euro2              ',
  '      tq.euro1               = :euro1              ',
  '      tq.cents50             = :cents50            ',
  '      tq.cents20             = :cents20            ',
  '      tq.cents10             = :cents10            ',
  '      tq.cents5              = :cents5             ',
  '      tq.cents2              = :cents2             ',
  '      tq.cent1               = :cent1              ',
  '      tq.don_creditcard      = :don_creditcard     ',
  '      tq.don_cheque          = :don_cheque         ',
  '      tq.coins_money_bag_id  = :coins_money_bag_id ',
  '      tq.bills_money_bag_id  = :bills_money_bag_id ',
  '      tq.don_cb_total_number = :don_cb_total_number',
  '      tq.don_cheque_number   = :don_cheque_number  ',
  'WHERE tq.id                  = :id                 ',
  'AND   tq.ul_id               = :ul_id              '].join('\n');


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
 * Update an existing TroncQueteur with new data
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.processUpdateTroncQueteur = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Received Message : "+JSON.stringify(parsedObject));
  //{"id":5074,"ul_id":348,"queteur_id":4536,"point_quete_id":121,"tronc_id":290,"depart_theorique":"2019-01-28 16:00:00","depart":"2019-01-28 16:54:24","retour":"2019-01-28 16:54:29","comptage":null,"last_update":"2019-01-28 16:54:32","last_update_user_id":163,"euro500":0,"euro200":0,"euro100":0,"euro50":0,"euro20":0,"euro10":4,"euro5":3,"euro2":34,"euro1":5,"cents50":33,"cents20":4,"cents10":23,"cents5":43,"cents2":4,"cent1":3,"don_cheque":0,"don_creditcard":0,"foreign_coins":null,"foreign_banknote":null,"notes_depart_theorique":"","notes_retour":"","notes_retour_comptage_pieces":"","notes_update":"","deleted":false,"coins_money_bag_id":"","bills_money_bag_id":"","don_cb_total_number":0,"don_cheque_number":0}

  const queryObj = {
    query: queryStr,
    params: parsedObject
  };



  return bigquery
    .query(queryObj)
    .then((data) => {
      console.log("Query Successful " +JSON.stringify(data));
      //rows : [{"amount":367.63,"weight":2399.3,"time_spent_in_minutes":420}]

      const queteur = { queteur_id: parsedObject.queteur_id, ul_id:parsedObject.ul_id };
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
