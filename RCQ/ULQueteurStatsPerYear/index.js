'use strict';
const mysql     = require('mysql');

const {Firestore} = require('@google-cloud/firestore');
const firestore   = new Firestore ({projectId:process.env.TARGET_PROJECT_ID});
const settings    = {timestampsInSnapshots: true};
firestore.settings(settings);

const fsCollectionName = 'ul_queteur_stats_per_year';


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
  'SELECT NOW()'].join('\n');

exports.ULQueteurStatsPerYear = (event, context) => {

  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());
  const ul_id         = parsedObject.id;
  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!mysqlPool)
  {
    mysqlPool = mysql.createPool(mysqlConfig);
  }

  //delete current stats of the UL
  let deleteCollection = function(path)
  {
    console.log("removing documents on collection '"+path+"' for ul_id="+ul_id);
    // Get a new write batch
    let batch = firestore.batch();                                                    

    return firestore
      .collection(path)
      .where("ul_id", "==", ul_id)
      .get()
      .then(
      querySnapshot => {
        console.log(`Start of deletion : '${querySnapshot.size}'`);
        querySnapshot.forEach(documentSnapshot => {
          console.log(`Found ${querySnapshot.size} documents at ${documentSnapshot.ref.path}`);
          batch.delete(documentSnapshot.ref);
        });
        console.log("commit of deletion");
        return batch.commit();
      });
  };


  return new Promise((resolve, reject) => {

    deleteCollection(fsCollectionName).then(
      ()=>
      {
        console.log("running query for UL : "+ul_id);
        mysqlPool.query(
          queryStr,
          [ul_id],
          (err, results) => {
            console.log(" query results part" );
            if (err)
            {
              console.error(err);
              reject(err);
            }
            else
            {
              if(results !== undefined && Array.isArray(results) && results.length >= 1)
              {
                const batch       = firestore.batch();
                const collection  = firestore.collection(fsCollectionName);
                let i = 0;
                results.forEach(
                  (row) =>
                  {
                    console.log("ULQueteurStatsPerYear : inserting row for UL "+ul_id+" "+JSON.stringify(row));
                    const docRef = collection.doc();
                    //otherwise we get this error from firestore : Firestore doesn’t support JavaScript objects with custom prototypes (i.e. objects that were created via the “new” operator)
                    batch.set(docRef, JSON.parse(JSON.stringify(row)));
                  });

                return batch.commit().then(() => {

                  let logMessage = "ULQueteurStatsPerYear for UL='"+parsedObject.name+"'("+ul_id+") : "+i+" rows inserted";

                  console.log(logMessage);
                  resolve(logMessage);
                });
              }
            }
          });
      });
  });
};