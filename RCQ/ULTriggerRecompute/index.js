'use strict';
const common            = require('./common');
const common_mysql      = require('./common_mysql');
const common_cloudTask  = require('./common_cloudTasks');

const topicName       = 'ul_update';

const queryStr = `
SELECT u.id, u.name, u.phone, u.latitude, u.longitude, u.address, u.postal_code, u.city, u.external_id, u.email, 
       u.id_structure_rattachement, u.date_demarrage_activite, u.date_demarrage_rcq, u.publicDashboard, u.spotfire_access_token
FROM   ul u
WHERE  u.date_demarrage_rcq is not null
ORDER BY date_demarrage_rcq
`;

let projectName     = common.getProjectName();
let serviceAccount  = "cf-computeulstats@"+projectName+".gserviceaccount.com";//"cf-ultriggerrecompute@"+projectName+".iam.gserviceaccount.com";//"service-1022015855967@gcp-sa-cloudtasks.iam.gserviceaccount.com";
let url             = "https://europe-west1-"+projectName+".cloudfunctions.net/ComputeULStats";
//format:
//SA : cf-ztestCrossProjectFirestoCx@rcq-fr-dev.iam.gserviceaccount.com
//URL: https://europe-west1-rcq-fr-dev.cloudfunctions.net/ComputeULStats


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

        await common.logDebug("query Results", results);
        //let logMessage = "Start processing UL array of size :"+results.length;
        //console.error(logMessage);
        if(results !== undefined && Array.isArray(results) && results.length >= 1)
        {
          // tasks for Queteur Stats
          for(let i=0;i<results.length;i++)
          {
            results[i].computeType='queteurStats';

            common.logDebug("data for queteurStats tasks "+i,results[i]);
            //await common_cloudTask.createTask(url, serviceAccount, results[i]);
          }
          // tasks for UL stats
          for(let i=0;i<results.length;i++)
          {
            results[i].computeType='ULStats';
            common.logDebug("data for ULStats tasks "+i,results[i]);
            //await common_cloudTask.createTask(url, serviceAccount, results[i]);
          }

          common.logDebug("ULTriggerRecompute - creating 2x"+results.length+" tasks ", {url:url, serviceAccount:serviceAccount, results:results});
          resolve("ULTriggerRecompute - creating "+results.length+" tasks ");
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
