'use strict';
const mysql     = require('mysql');

const Firestore = require('@google-cloud/firestore');
const firestore = new Firestore();

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();





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



const queryStr = [
  'SELECT  q.`id`,                                          ',
  '        q.`email`,                                       ',
  '        q.`first_name`,                                  ',
  '        q.`last_name`,                                   ',
  '        q.`secteur`,                                     ',
  '        q.`nivol`,                                       ',
  '        q.`mobile`,                                      ',
  '        q.`created`,                                     ',
  '        q.`updated`,                                     ',
  '        q.`notes`,                                       ',
  '        q.`ul_id`,                                       ',
  '        q.`active`,                                      ',
  '        q.`man`,                                         ',
  '        q.`birthdate`,                                   ',
  '        q.`qr_code_printed`,                             ',
  '        q.`referent_volunteer`,                          ',
  '        q.`anonymization_token`,                         ',
  '        q.`anonymization_date`,                          ',
  '        u.`name`       as ul_name,                       ',
  '        u.`latitude`   as ul_latitude,                   ',
  '        u.`longitude`  as ul_longitude                   ',
  'FROM `queteur` as q,                                     ',
  '     `ul`      as u                                      ',
  'WHERE q.ul_id = u.id                                     ',
  'AND   u.id    = ?                                        ',
  'AND   q.id    = ?                                        '].join('\n');


// [START findQueteurById]
// retrieve Queteur Info from it's ID in RCQ DB
exports.findQueteurById = functions.https.onCall((data, context) => {
  // [START_EXCLUDE]
  // [START readMessageData]
      //use only the user Id to retrieve it's queteur_id
  // [END readMessageData]
  // [START messageHttpsErrors]

  // Checking that the user is authenticated.
  if (!context.auth)
  {
    // Throwing an HttpsError so that the client gets the error details.
    throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
  }

  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!mysqlPool)
  {

    if(connectionName === null)
    {
      throw new functions.https.HttpsError('internal', 'env var not defined : INSTANCE_CONNECTION_NAME');
    }
    if(dbUser         === null)
    {
      throw new functions.https.HttpsError('internal', 'env var not defined : SQL_USER'                );
    }
    if(dbPassword     === null)
    {
      throw new functions.https.HttpsError('internal', 'env var not defined : SQL_PASSWORD'            );
    }
    if( dbName        === null)
    {
      throw new functions.https.HttpsError('internal', 'env var not defined : SQL_DB_NAME'             );
    }

    mysqlPool = mysql.createPool(mysqlConfig);
  }

  // [END messageHttpsErrors]

  // [START authIntegration]
  // Authentication / user information is automatically added to the request.
  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  console.log("uid='"+uid+"', name='"+name+"', email='"+email+"'");
  // [END authIntegration]

  // [START returnMessageAsync]
  // Saving the new message to the Realtime Database.

  return firestore
  .collection('queteurs')
  .doc(uid)
  .get()
  .then(queteurPromise => {
    if (queteurPromise.exists)
    {
      let queteurData = queteurPromise.data();
      let queteurId = queteurData.queteur_id;
      let ulId = queteurData.ul_id;

      return new Promise((resolve, reject) => {
        mysqlPool.query(
          queryStr,
          [ulId, queteurId],
          (err, results) => {
            if (err)
            {
              console.error(err);
              reject(err);
            }
            else
            {
              if(results !== undefined && Array.isArray(results) && results.length === 1)
              {
                console.debug("found queteur with id '"+queteurId+"' and ul_id='"+ulId+"' for firestore queteurs(uid='"+uid+"') : " +JSON.stringify(results[0]));
                resolve(JSON.stringify(results[0]));
              }
              else
              {
                let message= "no queteur found with id '"+queteurId+"' and ul_id='"+ulId+"' for firestore queteurs(uid='"+uid+"'), results : " + JSON.stringify(results);
                console.error(message);
                reject(message);
              }
            }
          });
      })

    }
    else
    {
      throw new functions.https.HttpsError('not-found', "queteur with uid='"+uid+" not found");
    }
  })
  .catch(function(error) {
    console.log("Error while getting current user document in queteur collection, with id='"+uid+"' "+JSON.stringify(error));
    throw new functions.https.HttpsError('unknown', error.message, error);
  });

  // [END_EXCLUDE]
});
// [END messageFunctionTrigger]