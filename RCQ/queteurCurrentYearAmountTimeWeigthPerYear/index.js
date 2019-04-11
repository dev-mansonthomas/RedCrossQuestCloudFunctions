const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = ['select EXTRACT(YEAR from tq.depart) as year,',
  'SUM(',
  '  tq.euro2   * 2    +',
  '  tq.euro1   * 1    +',
  '  tq.cents50 * 0.5  +',
  '  tq.cents20 * 0.2  +',
  '  tq.cents10 * 0.1  +',
  '  tq.cents5  * 0.05 +',
  '  tq.cents2  * 0.02 +',
  '  tq.cent1   * 0.01 +',
  '  tq.euro5   * 5    +',
  '  tq.euro10  * 10   +',
  '  tq.euro20  * 20   +',
  '  tq.euro50  * 50   +',
  '  tq.euro100 * 100  +',
  '  tq.euro200 * 200  +',
  '  tq.euro500 * 500  +',
  '  tq.don_cheque     +',
  '  tq.don_creditcard ',
  ') as amount,',
  'SUM( ',
  '  tq.euro500 *  1.1 +',
  '  tq.euro200 *  1.1 +',
  '  tq.euro100 *  1 +',
  '  tq.euro50  *  0.9 +',
  '  tq.euro20  *  0.8 +',
  '  tq.euro10  *  0.7 +',
  '  tq.euro5   *  0.6 +',
  '  tq.euro2   *  8.5 +',
  '  tq.euro1   *  7.5 +',
  '  tq.cents50 *  7.8 +',
  '  tq.cents20 *  5.74 +',
  '  tq.cents10 *  4.1 +',
  '  tq.cents5  *  3.92 +',
  '  tq.cents2  *  3.06 +',
  '  tq.cent1   *  2.3',
  ') as weight,',
  'SUM(DATETIME_DIFF(tq.retour , tq.depart, MINUTE)) as time_spent_in_minutes',
  'from  `redcrossquest.tronc_queteur` as tq',
  'where tq.queteur_id = @queteur_id',
  'AND   tq.deleted    = false' +
  'group by year' +
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
exports.queteurCurrentYearAmountTimeWeigthPerYear = (event, context) => {
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
