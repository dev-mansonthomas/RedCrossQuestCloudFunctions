const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = ['select count(distinct(tq.point_quete_id)) as nb_pt_quete,',
  'q.id,',
  'q.first_name,',
  'q.last_name',
  'from `redcrossquest.tronc_queteur` as tq,',
  '     `redcrossquest.queteur`       as q',
  'where tq.ul_id =2',
  'AND tq.queteur_id = q.id',
  'AND  q.active     = true  ',
  'AND tq.deleted    = false ',
  'AND EXTRACT(YEAR from tq.depart) = EXTRACT(YEAR from CURRENT_DATE())',
  'group by q.id, q.first_name, q.last_name',
  'order by nb_pt_quete desc;'].join('\n');


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
exports.ULRankingByNbPointQueteCurrentYear = (event, context) => {
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
