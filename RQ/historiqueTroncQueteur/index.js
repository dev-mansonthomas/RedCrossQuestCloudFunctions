'use strict';
const mysql     = require('mysql');
const moment    = require('moment');

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
  'SELECT                                ',
  '  tq.id,                              ',
  '  tq.point_quete_id,                  ',
  '  pq.name as point_quete,             ',
  '  tq.tronc_id,                        ',
  '  tq.depart_theorique,                ',
  '  tq.depart,                          ',
  '  tq.retour,                          ',
  '  tq.comptage,                        ',
  '  (tq.euro2*2     +                   ',
  '   tq.euro1*1     +                   ',
  '   tq.cents50*0.5 +                   ',
  '   tq.cents20*0.2 +                   ',
  '   tq.cents10*0.1 +                   ',
  '   tq.cents5*0.05 +                   ',
  '   tq.cents2*0.02 +                   ',
  '   tq.cent1*0.01  +                   ',
  '   tq.euro5*5     +                   ',
  '   tq.euro10*10   +                   ',
  '   tq.euro20*20   +                   ',
  '   tq.euro50*50   +                   ',
  '   tq.euro100*100 +                   ',
  '   tq.euro200*200 +                   ',
  '   tq.euro500*500 +                   ',
  '   tq.don_cheque  +                   ',
  '   tq.don_creditcard                  ',
  '  ) as amount,                        ',
  '  ( tq.euro500 * 1.1  +               ',
  '   tq.euro200  * 1.1  +               ',
  '   tq.euro100  * 1    +               ',
  '   tq.euro50   * 0.9  +               ',
  '   tq.euro20   * 0.8  +               ',
  '   tq.euro10   * 0.7  +               ',
  '   tq.euro5    * 0.6  +               ',
  '   tq.euro2    * 8.5  +               ',
  '   tq.euro1    * 7.5  +               ',
  '   tq.cents50  * 7.8  +               ',
  '   tq.cents20  * 5.74 +               ',
  '   tq.cents10  * 4.1  +               ',
  '   tq.cents5   * 3.92 +               ',
  '   tq.cents2   * 3.06 +               ',
  '   tq.cent1    * 2.3                  ',
  '  ) as weight,                        ',
  '  (timestampdiff(                     ',
  '    second,                           ',
  '    depart,                           ',
  '    retour))/3600                     ',
  '    as time_spent_in_hours,           ',
  '  tq.don_creditcard,                  ',
  '  tq.don_cheque                       ',
  '  FROM tronc_queteur as tq,           ',
  '       point_quete   as pq,           ',
  '       queteur       as q             ',
  'WHERE  q.id             = ?           ',
  'AND   tq.queteur_id     = q.id        ',
  'AND    q.ul_id          = ?           ',
  'AND   tq.point_quete_id = pq.id       ',
  'AND   YEAR(tq.depart)   = YEAR(NOW()) ',
  'AND   tq.comptage       IS NOT NULL   ',
  'AND   tq.deleted        = 0           ',
  'ORDER BY tq.id DESC;                  '].join('\n');


// [START findQueteurById]
// retrieve Queteur Info from it's ID in RCQ DB
exports.historiqueTroncQueteur = functions.https.onCall((data, context) => {
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

  return new Promise((resolve, reject) => {

    let queteurRef = firestore
      .collection('queteurs')
      .doc(uid);

    queteurRef
      .get()
      .then(queteurPromise => {
        if (queteurPromise.exists)
        {
          let queteurData   = queteurPromise.data() ;
          let queteurId     = queteurData.queteur_id;
          let ulId          = queteurData.ul_id     ;
          let historiqueTQ  = queteurData.historiqueTQ;
          let historiqueTQLastUpdate = queteurData.historiqueTQLastUpdate;


          //Retrieve the data from cache if the lastUpdate date is not older than 5 minutes and the data store contains at least one element.
          //if 0 element, we'll allow update every minute

          //console.log( JSON.stringify(historiqueTQLastUpdate)+);
          if( historiqueTQLastUpdate != null &&
              moment(historiqueTQLastUpdate).diff(moment(), 'seconds') <= 5*60 &&
              Array.isArray(historiqueTQ) &&
             (historiqueTQ.length > 0 || moment(historiqueTQLastUpdate).diff(moment(), 'seconds') <= 60)
          )
          {// if the data is fresh enough, return the cached data
            console.log("historique tronc_queteur retrieved from cache "+JSON.stringify(queteurData));
            resolve(historiqueTQ);
          }
          else
          {// cache miss or data too old
            console.log("historique tronc_queteur cache miss for queteur id "+queteurId);

            mysqlPool.query(
              queryStr,
              [queteurId, ulId],
              (err, results) => {
                if (err)
                {
                  console.error(err);
                  reject(err);
                }
                else
                {
                  queteurRef.update({'historiqueTQ':results, 'historiqueTQLastUpdate': new Date()}).then(()=>{
                    resolve(results);
                  }).catch(updateError=>{
                    console.error(updateError);
                    reject(updateError);
                  })
                }
              }
            );
          }
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


  });




  // [END_EXCLUDE]
});
// [END messageFunctionTrigger]