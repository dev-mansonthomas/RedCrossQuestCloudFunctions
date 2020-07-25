'use strict';
const common              = require('./common');
const common_firestore    = require('./common_firestore');

const {PubSub}        = require('@google-cloud/pubsub');
const topicName       = 'ul_update';
const pubsubClient    = new PubSub();

const fsCollectionName = 'ul_queteur_stats_per_year';

const queryStr = `
  select                                                                                            
    tq.ul_id,                                                                                       
    tq.queteur_id,                                                                                  
    SUM(                                                                                            
      tq.euro2   * 2    +                                                                           
      tq.euro1   * 1    +                                                                           
      tq.cents50 * 0.5  +                                                                           
      tq.cents20 * 0.2  +                                                                           
      tq.cents10 * 0.1  +                                                                           
      tq.cents5  * 0.05 +                                                                           
      tq.cents2  * 0.02 +                                                                           
      tq.cent1   * 0.01 +                                                                           
      tq.euro5   * 5    +                                                                           
      tq.euro10  * 10   +                                                                           
      tq.euro20  * 20   +                                                                           
      tq.euro50  * 50   +                                                                           
      tq.euro100 * 100  +                                                                           
      tq.euro200 * 200  +                                                                           
      tq.euro500 * 500  +                                                                           
      tq.don_cheque     +                                                                           
      tq.don_creditcard                                                                             
    ) as amount,                                                                                    
    SUM(tq.don_creditcard) as amount_cb,                                                            
    (select  amount                                                                                 
     from    yearly_goal yg                                                                         
     where   yg.ul_id = tq.ul_id                                                                    
     and     year = EXTRACT(YEAR from max(tq.depart))) amount_year_objective,                       
    SUM((                                                                                           
      tq.euro500 * 1.1  +                                                                           
      tq.euro200 * 1.1  +                                                                           
      tq.euro100 * 1    +                                                                           
      tq.euro50  * 0.9  +                                                                           
      tq.euro20  * 0.8  +                                                                           
      tq.euro10  * 0.7  +                                                                           
      tq.euro5   * 0.6  +                                                                           
      tq.euro2   * 8.5  +                                                                           
      tq.euro1   * 7.5  +                                                                           
      tq.cents50 * 7.8  +                                                                           
      tq.cents20 * 5.74 +                                                                           
      tq.cents10 * 4.1  +                                                                           
      tq.cents5  * 3.92 +                                                                           
      tq.cents2  * 3.06 +                                                                           
      tq.cent1   * 2.3)                                                                             
    ) as weight,                                                                                    
    SUM(TIMESTAMPDIFF(MINUTE, tq.depart, tq.retour )) as time_spent_in_minutes,                     
    count(1)                           as number_of_tronc_queteur,                                  
    count(distinct(tq.point_quete_id)) as number_of_point_quete,                                    
    (select count(1) from point_quete pq where pq.ul_id = tq.ul_id) as total_number_of_point_quete, 
    (select count(distinct(EXTRACT(DAY from tqq.depart)))                                           
     from tronc_queteur tqq                                                                         
     where tqq.queteur_id = tq.queteur_id                                                           
     and EXTRACT(YEAR from tqq.depart) = EXTRACT(YEAR from tq.depart)) as number_of_days_quete,     
    q.first_name,                                                                                   
    q.last_name,                                                                                    
    EXTRACT(YEAR from tq.depart) as year                                                            
  from tronc_queteur as tq,                                                                       
       queteur       as q                                                                         
  where tq.ul_id      = ?                                                                           
  AND   tq.queteur_id = q.id                                                                        
  AND    q.active     = true                                                                        
  AND   tq.deleted    = false                                                                       
  AND   tq.comptage is not null                                                                     
  group by tq.ul_id, tq.queteur_id, q.first_name, q.last_name,  year                                
`;

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

/***
 * Cloud Scheduler "trigger_ul_update" publish an empty message on "trigger_ul_update"
 * ULTriggerRecompute Cloud Function is listening, on reception it will trigger stats recompute every 400ms
 * which call this cloud function.
 *
 * Requires :
 * roles/cloudsql.client (ou roles/cloudsql.viewer)
 * roles/datastore.user on RQ Firestore
 *
 * data Recieved :
 * const data = {currentIndex:0, uls:results};
 *
 * */
exports.ULQueteurStatsPerYear = async (event, context) => {

  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());
  const uls           = parsedObject.uls;
  let   currentIndex  = parsedObject.currentIndex;

  if(!Array.isArray(uls))
  {
    console.error("uls is not an array", uls);
    return;
  }
  if(currentIndex >= uls.length )
  {
    console.error("currentIndex is greater than array size", uls);
    return;
  }

  const ul_id         = uls[currentIndex].id;
  const ul_name       = uls[currentIndex].name;

  let mysqlPool = await common.initMySQL('MYSQL_USER_READ');

  //delete current stats of the UL
  let deleteCollection = function(path)
  {
    console.log("removing documents on collection '"+path+"' for ul_id="+ul_id);
    // Get a new write batch
    let batch = common_firestore.firestore.batch();

    return common_firestore.firestore
      .collection(path)
      .where("ul_id", "==", ul_id)
      .get()
      .then(
        querySnapshot => {
          console.log(`Start of deletion : '${querySnapshot.size}' documents for UL '${ul_id}'`);
          querySnapshot.forEach(documentSnapshot => {
            batch.delete(documentSnapshot.ref);
          });
          console.log("commit of deletion for UL '${ul_id}'");
          return batch.commit();
        });
  };


  return new Promise((resolve, reject) => {

    deleteCollection(fsCollectionName).then(
      ()=>
      {
        mysqlPool.query(
          queryStr,
          [ul_id],
          (err, results) => {

            if (err)
            {
              console.error("Error while querying MySQL with query "+queryStr, err);
              reject(err);
            }
            else
            {
              if(Array.isArray(results) && results.length >= 1)
              {
                const batch       = common_firestore.firestore.batch();
                const collection  = common_firestore.firestore.collection(fsCollectionName);
                let i = 0;
                results.forEach(
                  (row) =>
                  {
                    const docRef = collection.doc();
                    //otherwise we get this error from firestore : Firestore doesn’t support JavaScript objects with custom prototypes (i.e. objects that were created via the “new” operator)
                    batch.set(docRef, JSON.parse(JSON.stringify(row)));
                    i++;
                  });

                return batch.commit().then(() => {

                  let logMessage = "ULQueteurStatsPerYear for UL='"+ul_name+"'("+ul_id+") : "+i+" rows inserted";
                  console.log(logMessage);

                  parsedObject.currentIndex = currentIndex++;
                  const newDataBuffer  = Buffer.from(JSON.stringify(parsedObject));  

                  pubsubClient
                    .topic     (topicName)
                    .publish   (newDataBuffer)
                    .then      ((dataResult)=>{
                      console.trace("Published 1 message to process next UL on topic '"+topicName+"' "+JSON.stringify(dataResult), parsedObject);
                      resolve(logMessage);
                    })
                    .catch(err=>{
                      handleError(err);
                    });
                });
              }
              else
              {
                let logMessage = "query for UL '"+ul_id+"' returned no row "+queryStr+" results : "+JSON.stringify(results);
                console.log(logMessage);
                resolve(logMessage);
              }
            }
          });
      });
  });
};
