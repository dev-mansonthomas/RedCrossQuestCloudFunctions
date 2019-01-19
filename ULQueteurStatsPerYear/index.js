const {BigQuery}  = require('@google-cloud/bigquery');
const {Firestore} = require('@google-cloud/firestore');

const bigquery   = new BigQuery  ();
const firestore  = new Firestore ();

const fsCollectionName = 'ul_queteur_stats_per_year';


const queryStr = [

  'select                 ',
  '  tq.ul_id,            ',
  '  tq.queteur_id,       ',
  '  count(1) tronc_count,',
  '  SUM(                 ',
  '    tq.euro2   * 2    +',
  '    tq.euro1   * 1    +',
  '    tq.cents50 * 0.5  +',
  '    tq.cents20 * 0.2  +',
  '    tq.cents10 * 0.1  +',
  '    tq.cents5  * 0.05 +',
  '    tq.cents2  * 0.02 +',
  '    tq.cent1   * 0.01 +',
  '    tq.euro5   * 5    +',
  '    tq.euro10  * 10   +',
  '    tq.euro20  * 20   +',
  '    tq.euro50  * 50   +',
  '    tq.euro100 * 100  +',
  '    tq.euro200 * 200  +',
  '    tq.euro500 * 500  +',
  '    tq.don_cheque     +',
  '    tq.don_creditcard  ',
  '  ) as amount,         ',
  '  SUM((                ',
  '    tq.euro500 * 1.1  +',
  '    tq.euro200 * 1.1  +',
  '    tq.euro100 * 1    +',
  '    tq.euro50  * 0.9  +',
  '    tq.euro20  * 0.8  +',
  '    tq.euro10  * 0.7  +',
  '    tq.euro5   * 0.6  +',
  '    tq.euro2   * 8.5  +',
  '    tq.euro1   * 7.5  +',
  '    tq.cents50 * 7.8  +',
  '    tq.cents20 * 5.74 +',
  '    tq.cents10 * 4.1  +',
  '    tq.cents5  * 3.92 +',
  '    tq.cents2  * 3.06 +',
  '    tq.cent1   * 2.3)  ',
  '  ) as weight,         ',
  '  SUM(DATETIME_DIFF(tq.retour , tq.depart, MINUTE)) as time_spent_in_minutes, ',
  '  count(distinct(tq.point_quete_id)) as unique_point_quete_count, ',
  '  q.first_name,',
  '  q.last_name, ',
  '  EXTRACT(YEAR from tq.depart) as year    ',
  'from `redcrossquest.tronc_queteur` as tq, ',
  '     `redcrossquest.queteur`       as q   ',
  'where tq.ul_id      = @ul_id              ',
  'AND   tq.queteur_id = q.id                ',
  'AND    q.active     = true                ',
  'AND   tq.deleted    = false               ',
  'group by tq.ul_id, tq.queteur_id, q.first_name, q.last_name,  year ',
  'order by amount desc '].join('\n');


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
exports.ULQueteurStatsPerYear = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Recieved Message : "+JSON.stringify(parsedObject));
  //{ ul_id:parsedObject.ul_id }

  const queryObj = {
    query: queryStr,
    params: {
      ul_id: parsedObject.ul_id
    }
  };

  bigquery
    .query(queryObj)
    .then((data) => {
      console.log("Query Successful, # rows : "+data.length+" data[0].length:"+data[0].length);
      //rows : [{"amount":367.63,"weight":2399.3,"time_spent_in_minutes":420}]
      const rows = data[0];
      console.log("Query Successful");

      const batch       = firestore.batch();

      console.log("Batch Created ");

      console.log("Getting Collection");
      const collection  = firestore.collection(fsCollectionName);
      console.log("Getting Collection '"+fsCollectionName+"' retrieved");

      for(let i=0;i<rows.length;i++)
      {
        console.log("getting a new DocId");

        const docRef = collection.doc();

        console.log("Adding to docRef='"+docRef.id+"' : "+JSON.stringify(rows[i]));
        batch.set(docRef, rows[i]);
        console.log("Added to batch");
      }

      console.log("Commiting batch insert");
      batch.commit().then(() => {
        console.log('Successfully executed batch');
      });

    })
    .catch(err => {
      handleError(err);
    });

};
