'use strict';
const common              = require('./common');
const common_firestore    = require('./common_firestore');
const common_mysql        = require('./common_mysql');
const common_pubsub       = require('./common_pubsub');
const chunk               = require('lodash.chunk');

const {PubSub}        = require('@google-cloud/pubsub');
const topicName       = 'ul_update';
const pubsubClient    = new PubSub();

const fsCollectionName = 'ul_stats_current_year';

const queryStr = `
    select
        tq.ul_id,
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
        EXTRACT(YEAR from tq.depart) as year
    from tronc_queteur as tq
    where tq.ul_id            = ?
      AND   tq.deleted        = false
      AND   tq.comptage       is not null
      AND   YEAR(tq.depart)   = YEAR(NOW())
    group by tq.ul_id;                              
`;

/***
 * Cloud Scheduler "trigger_ul_update" publish an empty message on "trigger_ul_update"
 * ULTriggerRecompute Cloud Function is listening, on reception it will query the list of active UL 
 * and post a message with this list and an index set to 0
 * 
 * This cloud function receive the message via 'ul_update' topic, make the computation and store it in Firestore,
 * and then republish the same message with the index incremented
 *
 * Requires :
 * roles/cloudsql.client (ou roles/cloudsql.viewer)
 * roles/datastore.user on RQ Firestore
 *
 * data Recieved :
 * const data = {currentIndex:0, uls:results};
 *
 * */
exports.ULStatsCurrentYear = async (event, context) => {

  const pubsubMessage = event.data;
  let   parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  common.logDebug("ULStatsCurrentYear - start processing", parsedObject);

  const uls           = parsedObject.uls;
  let   currentIndex  = parsedObject.currentIndex;

  if(!Array.isArray(uls))
  {
    common.logError("ULStatsCurrentYear - uls is not an array", uls);
    return;
  }
  if(currentIndex >= uls.length )
  {
    common.logError("ULStatsCurrentYear - currentIndex is greater than array size", uls);
    return;
  }

  const ul_id         = uls[currentIndex].id;
  const ul_name       = uls[currentIndex].name;

  let mysqlPool = await common_mysql.initMySQL('MYSQL_USER_READ');

  //delete current stats of the UL
  let deleteCollection = async function(path)
  {
    common.logInfo("ULStatsCurrentYear - removing documents on collection '"+path+"' for ul_id="+ul_id);
    // Get a new write batch

    const documents =  await common_firestore.firestore
      .collection(path)
      .where("ul_id", "==", ul_id)
      .get();
    let i = 0;
    const batches = chunk(documents.docs, 500).map( docs =>{
       const batch =  common_firestore.firestore.batch();
       common.logDebug("ULStatsCurrentYear - Starting a new batch of deletion at index "+i);

      docs.forEach(doc =>{
        batch.delete(doc.ref);
        i++;
      });
      common.logDebug("ULStatsCurrentYear - Committing a batch of deletion at index "+i);
      return batch.commit();
    });

    return Promise.all(batches);
  };


  return new Promise((resolve, reject) => {

    deleteCollection(fsCollectionName).then(
      ()=>
      {
        const queryArgs = [ul_id];
        mysqlPool.query(
          queryStr,
          ul_id,
          async (err, results) => {

            if (err)
            {
              common.logError("ULStatsCurrentYear - error while running query ", {queryStr:queryStr, mysqlArgs:queryArgs, exception:err});
              reject(err);
            }
            else
            {
              if(Array.isArray(results) && results.length >= 1)
              {
                const collection  = common_firestore.firestore.collection(fsCollectionName);

                let i = 0;
                const batches = chunk(results, 500).map( docs =>{
                  const batch =  common_firestore.firestore.batch();
                  common.logDebug("ULStatsCurrentYear - Starting a new batch of insertion at index "+i);

                  docs.forEach(doc =>{
                    const docRef = collection.doc();
                    batch.set(docRef, JSON.parse(JSON.stringify(doc)));
                    i++;
                  });
                  common.logDebug("ULStatsCurrentYear - Committing a batch of insertion at index "+i);
                  return batch.commit();
                });

                return Promise.all(batches).then(async () => {

                  let logMessage = "ULStatsCurrentYear for UL='"+ul_name+"'("+ul_id+") : "+i+" rows inserted";
                  common.logDebug(logMessage);
                  parsedObject.currentIndex = currentIndex+1;
                  await common_pubsub.publishMessage(topicName, parsedObject)
                  resolve(logMessage);
                });
              }
              else
              {
                let logMessage = "ULStatsCurrentYear - query for UL '"+ul_id+"' returned no row "+queryStr+" results : "+JSON.stringify(results);
                common.logInfo(logMessage);
                
                parsedObject.currentIndex = currentIndex+1;
                await common_pubsub.publishMessage(topicName, parsedObject)

                resolve(logMessage);
              }
            }
          });
      });
  });
};
