const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = ['select ',
  'tq.queteur_id,',
  'SUM(DATETIME_DIFF(tq.retour , tq.depart, MINUTE)) as time_spent_in_minutes',
  ' q.first_name,',
  ' q.last_name'  ,
  'from `redcrossquest.tronc_queteur` as tq,',
  '     `redcrossquest.queteur`       as q'  ,
  'where tq.ul_id    = @ul_id',
  'AND tq.queteur_id = q.id'  ,
  'AND  q.active     = true'  ,
  'AND tq.deleted    = false' ,
  'group by tq.queteur_id, q.first_name, q.last_name',
  'order by amount desc'].join('\n');


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
exports.ULRankingByTime = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Recieved Message : "+JSON.stringify(parsedObject));
  //{ queteur_id: parsedObject.queteur_id, ul_id:parsedObject.ul_id }

  const queryObj = {
    query: queryStr,
    params: {
      ul_id: parsedObject.ul_id
    }
  };

  bigquery
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
