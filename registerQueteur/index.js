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

  mysqlPool.query('SELECT NOW() AS now',
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