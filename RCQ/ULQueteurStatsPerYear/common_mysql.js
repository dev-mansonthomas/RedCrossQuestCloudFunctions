'use strict';
const common         = require('./common');

const connectionName = process.env.INSTANCE_CONNECTION_NAME || null;
const dbUser         = process.env.SQL_USER                 || null;
const dbName         = process.env.SQL_DB_NAME              || null;

if(connectionName === null)
{
  throw new Error('env var not defined : INSTANCE_CONNECTION_NAME');
}
if(dbUser         === null)
{
  throw new Error('env var not defined : SQL_USER'                );
}
if( dbName        === null)
{
  throw new Error('env var not defined : SQL_DB_NAME'             );
}

const mysql                        = require('mysql');

const mysqlConfig = {
  connectionLimit : 1,
  user            : dbUser,
  database        : dbName,
};
if (process.env.NODE_ENV === 'production') {
  mysqlConfig.socketPath = `/cloudsql/${connectionName}`;
}

// Connection pools reuse connections between invocations,
// and handle dropped or expired connections automatically.
let mysqlPool;

//: Promise<T>
async function initMySQL(secretName) {
// Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!module.exports.mysqlPool)
  {
    // Access the secret.
    mysqlConfig.password     = await getSecret(secretName);
    module.exports.mysqlPool = mysql.createPool(mysqlConfig);
  }
  return module.exports.mysqlPool;
}

module.exports = {
  initMySQL   : initMySQL,
  mysqlPool   : mysqlPool
};
