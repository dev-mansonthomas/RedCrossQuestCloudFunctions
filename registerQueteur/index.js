'use strict';
const mysql     = require('mysql');
const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();



const connectionName = process.env.INSTANCE_CONNECTION_NAME || throw new functions.https.HttpsError('internal', 'env var not defined : INSTANCE_CONNECTION_NAME');
const dbUser         = process.env.SQL_USER                 || throw new functions.https.HttpsError('internal', 'env var not defined : SQL_USER'                );
const dbPassword     = process.env.SQL_PASSWORD             || throw new functions.https.HttpsError('internal', 'env var not defined : SQL_PASSWORD'            );
const dbName         = process.env.SQL_DB_NAME              || throw new functions.https.HttpsError('internal', 'env var not defined : SQL_DB_NAME'             );

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


exports.registerQueteur = functions.https.onCall((data, context) => {

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
    mysqlPool = mysql.createPool(mysqlConfig);
  }


  let first_name           = data.first_name           ;
  let last_name            = data.last_name            ;
  let man                  = data.man === 1            ;
  let birthdate            = data.birthdate            ;
  let email                = data.email                ;
  let secteur              = data.secteur              ;
  let nivol                = data.nivol                ;
  let mobile               = data.mobile               ;
  let ul_registration_token= data.ul_registration_token;



  const queryStr = `
INSERT INTO \`queteur_registration\`
(\`first_name\`,\`last_name\`,\`man\`,\`birthdate\`,\`email\`,\`secteur\`,\`nivol\`,\`mobile\`,\`created\`,\`ul_registration_token\`)
VALUES
( ?,?,?,?,?,?,?,?,NOW(),?)
`;

  mysqlPool.query(queryStr,
    [first_name, last_name, man, birthdate, email, secteur, nivol, mobile, ul_registration_token],
    (err, results) => {
      if (err)
      {
        console.error(err);
        throw new functions.https.HttpsError('unknown', err.message, err);
      }
      else
      {
        return JSON.stringify(results);
      }
    });

  // Close any SQL resources that were declared inside this function.
  // Keep any declared in global scope (e.g. mysqlPool) for later reuse.

});


