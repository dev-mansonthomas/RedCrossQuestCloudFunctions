const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = ['select ',
  'tq.queteur_id,',
  'SUM(',
  '  tq.euro500 *  1.1  +',
  '  tq.euro200 *  1.1  +',
  '  tq.euro100 *  1    +',
  '  tq.euro50  *  0.9  +',
  '  tq.euro20  *  0.8  +',
  '  tq.euro10  *  0.7  +',
  '  tq.euro5   *  0.6  +',
  '  tq.euro2   *  8.5  +',
  '  tq.euro1   *  7.5  +',
  '  tq.cents50 *  7.8  +',
  '  tq.cents20 *  5.74 +',
  '  tq.cents10 *  4.1  +',
  '  tq.cents5  *  3.92 +',
  '  tq.cents2  *  3.06 +',
  '  tq.cent1   *  2.3'   ,
  ') as weight,'  ,
  ' q.first_name,',
  ' q.last_name'  ,
  'from `redcrossquest.tronc_queteur` as tq,',
  '     `redcrossquest.queteur`       as q'  ,
  'where tq.ul_id    = @ul_id',
  'AND tq.queteur_id = q.id'  ,
  'AND  q.active     = true'  ,
  'AND tq.deleted    = false' ,
  'AND EXTRACT(YEAR from tq.depart) = EXTRACT(YEAR from CURRENT_DATE())',
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
exports.ULRankingByWeightCurrentYear = (event, context) => {
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
