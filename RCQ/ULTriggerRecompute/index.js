'use strict';
const common        = require('./common');
const common_mysql  = require('./common_mysql');
const common_pubsub = require('./common_pubsub');

const topicName       = 'ul_update';

const queryStr = `
SELECT u.id, u.name, u.phone, u.latitude, u.longitude, u.address, u.postal_code, u.city, u.external_id, u.email, 
       u.id_structure_rattachement, u.date_demarrage_activite, u.date_demarrage_rcq, u.publicDashboard, u.spotfire_access_token
FROM   ul u
WHERE  u.date_demarrage_rcq is not null
ORDER BY date_demarrage_rcq
`;

/**
 * Cloud Scheduler "trigger_ul_update" publish an empty message on "trigger_ul_update"
 * This function : ULTriggerRecompute Cloud Function is listening
 *
 * roles :  roles/cloudsql.client;roles/secretmanager.secretAccessor;roles/pubsub.publisher;roles/pubsub.subscriber
 **/
exports.ULTriggerRecompute = async (event, context) => {


  common.logDebug("ULTriggerRecompute - start of processing");
  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  let mysqlPool = await common_mysql.initMySQL('MYSQL_USER_READ');

  return new Promise( (resolve, reject) => {
    mysqlPool.query(queryStr, [],
      async (err, results) => {
      if (err)
      {
        common.logError("error while running query ", {queryStr:queryStr, mysqlArgs:[], exception:err});
        reject(err);
      }
      else
      {
        //let logMessage = "Start processing UL array of size :"+results.length;
        //console.error(logMessage);
        if(results !== undefined && Array.isArray(results) && results.length >= 1)
        {
          await common_pubsub.publishMessage(topicName, {currentIndex:0, uls:results})

          common.logDebug("Published 1 message on topic '"+topicName+"'", {dataResult:dataResult, data:data});
          resolve("ULTriggerRecompute done with "+results.length+" UL");
        }
        else
        {
          let logMessage="No UL enabled for RCQ found... weird... ";
          common.logError(logMessage);
          reject(logMessage);
        }
      }
    });
  });
};
