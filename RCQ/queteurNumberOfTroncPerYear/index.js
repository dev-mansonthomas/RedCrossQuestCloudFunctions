const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = ['select count(1), EXTRACT(YEAR from tq.depart) as year',
  'from `redcrossquest.tronc_queteur` as tq',
  'where tq.queteur_id = @queteur_id',
  'AND   tq.deleted    = false',
  'group by year ',
  'order by year asc'].join('\n');


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
exports.queteurNumberOfTroncPerYear = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Recieved Message : "+JSON.stringify(parsedObject));
  //{ queteur_id: parsedObject.queteur_id, ul_id:parsedObject.ul_id }

  const queryObj = {
    query: queryStr,
    params: {
      queteur_id: parsedObject.queteur_id
    }
  };

  return bigquery
    .query(queryObj)
    .then((data) => {
      console.log(JSON.stringify(data));
      const rows = data[0];
      //rows : [{"amount":367.63,"weight":2399.3,"time_spent_in_minutes":420}]
    })
    .catch(err => {
      handleError(err);
    });

};
