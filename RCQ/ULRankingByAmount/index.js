const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();





const queryStr = ['select ',
  'tq.queteur_id,',
  'SUM(',
  '  tq.euro2   * 2    ',
  '  tq.euro1   * 1    ',
  '  tq.cents50 * 0.5  ',
  '  tq.cents20 * 0.2  ',
  '  tq.cents10 * 0.1  ',
  '  tq.cents5  * 0.05 ',
  '  tq.cents2  * 0.02 ',
  '  tq.cent1   * 0.01 ',
  '  tq.euro5   * 5    ',
  '  tq.euro10  * 10   ',
  '  tq.euro20  * 20   ',
  '  tq.euro50  * 50   ',
  '  tq.euro100 * 100  ',
  '  tq.euro200 * 200  ',
  '  tq.euro500 * 500  ',
  '  tq.don_cheque     ',
  '  tq.don_creditcard ',
  ') as amount,'  ,
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
 * NOT USED
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.ULRankingByAmount = (event, context) => {
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
