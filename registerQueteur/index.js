'use strict';

const mysql = require('mysql');

const connectionName = process.env.INSTANCE_CONNECTION_NAME || '<YOUR INSTANCE CONNECTION NAME>';
const dbUser         = process.env.SQL_USER                 || '<YOUR DB USER>';
const dbPassword     = process.env.SQL_PASSWORD             || '<YOUR DB PASSWORD>';
const dbName         = process.env.SQL_DB_NAME              || '<YOUR DB NAME>';

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

exports.registerQueteur = (req, res) => {
  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!mysqlPool)
  {
    mysqlPool = mysql.createPool(mysqlConfig);
  }

  var first_name = 'XXX2';
  var last_name = 'XXX2';
  var man= 1;
  var birthdate='2019-03-11';
  var email='na@na.com';
  var secteur=1;
  var nivol='1A';
  var mobile='33631107592';
  var ul_registration_token='xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx';


  const queryStr = `
INSERT INTO \`queteur_registration\`
(\`first_name\`,\`last_name\`,\`man\`,\`birthdate\`,\`email\`,\`secteur\`,\`nivol\`,\`mobile\`,\`created\`,\`ul_registration_token\`)
VALUES
( ?,?,?,?,?,?,?,?,NOW(),?)
`;

  mysqlPool.query(queryStr,
    (err, results) => {
    if (err) {
      console.error(err);
      res.status(500).send(err);
    } else {
      res.send(JSON.stringify(results));
    }
  });

  // Close any SQL resources that were declared inside this function.
  // Keep any declared in global scope (e.g. mysqlPool) for later reuse.
};