'use strict';
const common    = require('./common');

const {PubSub}        = require('@google-cloud/pubsub');
const topicName       = 'ul_update';
const pubsubClient    = new PubSub();

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


  console.error("ULTriggerRecompute - start of processing");
  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  let mysqlPool = await common.initMySQL('MYSQL_USER_READ');

  return new Promise((resolve, reject) => {
    mysqlPool.query(queryStr, [],
    (err, results) => {
      if (err)
      {
        console.error(err);
        reject(err);
      }
      else
      {
        //let logMessage = "Start processing UL array of size :"+results.length;
        //console.error(logMessage);
        if(results !== undefined && Array.isArray(results) && results.length >= 1)
        {
          const data        = {currentIndex:0, uls:results};
          const dataBuffer  = Buffer.from(JSON.stringify(data));

          pubsubClient
            .topic     (topicName)
            .publish   (dataBuffer)
            .then      ((dataResult)=>{
              console.trace("Published 1 message on topic '"+topicName+"' "+JSON.stringify(dataResult), data);
              resolve("ULTriggerRecompute done with "+results.length+" UL");
            })
            .catch(err=>{
              handleError(err);
            });

        }
        else
        {
          let logMessage="No UL enabled for RCQ found... weird... ";
          console.error(logMessage);
          reject(logMessage);
        }
      }
    });
  });

};
