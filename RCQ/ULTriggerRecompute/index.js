'use strict';
const mysql     = require('mysql');
const {PubSub}  = require('@google-cloud/pubsub');

const topicName       = 'ul_update';
const pubsubClient    = new PubSub();

const publishBetweenUlDelay = 400;


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

const connectionName = process.env.INSTANCE_CONNECTION_NAME || null;
const dbUser         = process.env.SQL_USER                 || null;
const dbPassword     = process.env.SQL_PASSWORD             || null;
const dbName         = process.env.SQL_DB_NAME              || null;

const mysqlConfig = {
  connectionLimit : 1,
  user            : dbUser,
  password        : dbPassword,
  database        : dbName,
};
if (process.env.NODE_ENV === 'production') {
  mysqlConfig.socketPath = `/cloudsql/${connectionName}`;
}

// Connection pools reuse connections between invocations,
// and handle dropped or expired connections automatically.
let mysqlPool;

const queryStr = `
SELECT u.id, u.name, u.phone, u.latitude, u.longitude, u.address, u.postal_code, u.city, u.external_id, u.email, 
       u.id_structure_rattachement, u.date_demarrage_activite, u.date_demarrage_rcq, u.publicDashboard, u.spotfire_access_token,
       us.id as settings_id, us.settings
FROM   ul u,
       ul_settings us
WHERE  u.date_demarrage_rcq is not null
AND    u.id = us.ul_id
ORDER BY date_demarrage_rcq asc
`;

exports.ULTriggerRecompute = (event, context) => {


  console.error("start of processing");
  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!mysqlPool)
  {
    mysqlPool = mysql.createPool(mysqlConfig);
  }

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
          let i=0;
          results.forEach( (ul) => {
            i++;
            const dataBuffer    = Buffer.from(JSON.stringify(ul));
            // in order to not hammer the MySQL instance
            // the publish on the topic is done every 400 secondes
            // by taking the index (i) * 400 as a delay for setTimeout
            setTimeout(function()
                       {
                         pubsubClient
                         .topic     (topicName)
                         .publish   (dataBuffer)
                         .then      ((data)=>{
                           console.error("Published 1 message on topic '"+topicName+"' "+JSON.stringify(ul) + " data:"+JSON.stringify(data));

                         })
                         .catch(err=>{
                           handleError(err);
                         });
                       }, i*publishBetweenUlDelay);
          });

          let logMessage = "Number of UL triggered for recomputed : "+i+" results size :"+results.length;
          console.error(logMessage);

          //before returning resolve, we must wait for all the setTimeout to execute
          //A cloud function can't exceed 9minutes of exec : 1350messages at 1msg every 400ms
          setTimeout(function()
           {
             resolve(logMessage);
           }, (i+5)*publishBetweenUlDelay);

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