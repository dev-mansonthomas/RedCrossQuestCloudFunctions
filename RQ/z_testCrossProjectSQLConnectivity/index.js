'use strict';
const mysql     = require('mysql');

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



exports.z_testCrossProjectSQLConnectivity = (req, res) => {

  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!mysqlPool)
  {
    mysqlPool = mysql.createPool(mysqlConfig);
  }


  const queryStr = `
select count(1) as nb_tq
from tronc_queteur
`;

  return new Promise((resolve, reject) => {
    mysqlPool.query(queryStr, [],
                    (err, results) => {
                      if (err)
                      {
                        console.error(err);
                        res.status(500).send(JSON.stringify(err));
                        reject(err);
                      }
                      else
                      {
                        console.info("Number "+JSON.stringify(results));
                        res.status(200).send(JSON.stringify(results));
                        resolve(JSON.stringify(results));
                      }
                    });
  }).catch((err) =>{
    console.info(err.message, JSON.stringify(err));
    res.status(500).send(JSON.stringify(err));
  });

};