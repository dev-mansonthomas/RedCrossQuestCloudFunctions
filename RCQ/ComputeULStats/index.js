'use strict';
const common              = require('./common');

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
exports.ComputeULStats = async (request, response) => {

  const pubsubMessage = request.data;
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
  {//that's when we finish looping the collection of UL
    common.logDebug("ULStatsCurrentYear - currentIndex is greater than array size", uls);
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
                  common.logDebug("About to wait 400ms "+logMessage);
                  setTimeout(async function(){

                    common.logDebug("After waiting 400ms "+logMessage);
                    parsedObject.currentIndex = currentIndex+1;
                    const responseData = await common_pubsub.publishMessage(topicName, parsedObject);
                    common.logDebug("After waiting 400ms "+logMessage+". Publishing on topic "+topicName, {parsedObject:parsedObject, responseData:responseData});
                    resolve(logMessage);
                  }, 400);


                });
              }
              else
              {
                setTimeout(async function(){
                  let logMessage = "ULStatsCurrentYear - query for UL '"+ul_id+"' returned no row "+queryStr+" results : "+JSON.stringify(results);
                  common.logInfo(logMessage);

                  parsedObject.currentIndex = currentIndex+1;
                  await common_pubsub.publishMessage(topicName, parsedObject)

                  resolve(logMessage);
                }, 400);
              }
            }
          });
      });
  });
};
