'use strict';
const common              = require('./common');
const common_firestore    = require('./common_firestore');
const common_mysql        = require('./common_mysql');
const chunk               = require('lodash.chunk');

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();

const queryStr = `
    select id, ul_id, nivol, firebase_uid
    from queteur
    where firebase_uid is not null
      and id > 0
      and  length(nivol)> 0
      and active = 1
    order by ul_id, id;                  
`;


// [START resyncQueteurIdToFirestore]
/**
 * Fetch all active queteurs ID with a firebase_uid defined, and update firestore with it.
 * When restoring production to test environment, the id stored in firestore may not be in sync with the refreshed MySQL.
 * (as queteur and the RQ profile won't be created in sync with production)
 *
 * Permissions :
 * firestore read/write
 * MySQL read
 * */
exports.resyncQueteurIdToFirestore = functions.https.onRequest(async (data, context) => {


  let mysqlPool = await common_mysql.initMySQL('MYSQL_USER_READ');

  common.logDebug("resyncQueteurIdToFirestore - data", data);

  const queryArgs = [];
  mysqlPool.query(
    queryStr,
    queryArgs,
    (err, results) => {
      if (err)
      {
        common.logError("error while running query ", {queryStr:queryStr, mysqlArgs:queryArgs, exception:err});
        return err;
      }
      else
      {
        let documents = JSON.parse(JSON.stringify(results));

        let i = 0;
        const queteursCollection = common_firestore.firestore.collection('queteurs');
        const batches = chunk(documents, 500).map( docs =>{
          const batch =  common_firestore.firestore.batch();
          common.logDebug("resyncQueteurIdToFirestore - Starting a new batch of queteurId update at index "+i+" ");
          
          docs.forEach(doc =>{

            const q = queteursCollection.doc(doc['firebase_uid']);
            batch.update(q, {queteur_id:doc['queteur_id']});
            i++;
          });




          common.logDebug("resyncQueteurIdToFirestore - Committing a batch of deletion at index "+i+"");
          return batch.commit();
        });

        return Promise.all(batches);
      }
    }
  );

});
