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



const queryTmpl = [
  'UPDATE tronc_queteur     ',
  'SET    column     = ?    ',
  'WHERE  id         = ?    ',
  'AND    ul_id      = ?    ',
  'AND    queteur_id = ?    ',
  'AND    deleted    = false'].join('\n');


// [START findQueteurById]
// retrieve Queteur Info from it's ID in RCQ DB
exports.tronc_setDepartOrRetour = functions.https.onCall((data, context) => {
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

  let isDepart = data.isDepart;
  let date     = data.date;
  let tqId     = data.tqId;


  let queryStr = queryTmpl.replace('column', isDepart?'depart':'retour');
  return firestore
  .collection('queteurs')
  .doc(uid)
  .get()
  .then(queteurPromise => {
    if (queteurPromise.exists)
    {
      let queteurData = queteurPromise.data() ;
      let queteurId   = queteurData.queteur_id;
      let ulId        = queteurData.ul_id     ;

      return new Promise((resolve, reject) => {
        mysqlPool.query(
          queryStr,
          [date, tqId, ulId, queteurId],
          (err, results) => {
            if (err)
            {
              console.error(err);
              reject(err);
            }
            else
            {
              if(results !== undefined && result.affectedRows === 1)
              {
                console.debug(`Update Depart/Tronc (isDepart='${isDepart}') tqId='${tqId}', ulId='${ulId}' queteurId='${queteurId}' did update one row with query ${queryStr}`);
                resolve(JSON.stringify({success:true}));
              }
              else
              {

                console.error(`Update Depart/Tronc (isDepart='${isDepart}') tqId='${tqId}', ulId='${ulId}' queteurId='${queteurId}' did not update the correct number of row (ie : 1) with query ${queryStr} ${results.affectedRows}`);
                reject(JSON.stringify({success:false, message:`incorrect number of rows updated ${results.affectedRows}`}));
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