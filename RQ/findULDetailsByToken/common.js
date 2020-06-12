'use strict';
const mysql     = require('mysql');
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const secretManagerServiceClient = new SecretManagerServiceClient();

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


async function initMySQL(secretName: string): Promise<T> {
// Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!mysqlPool)
  {
    // Access the secret.
    mysqlConfig.password = await getSecret(secretName);
    mysqlPool = mysql.createPool(mysqlConfig);
  }
  return mysqlPool;
}

async function getSecret(secretName: string): Promise<T> {
  // Access the secret.
  const [accessResponse] = await secretManagerServiceClient.accessSecretVersion({name: secretName});
  return accessResponse.payload.data.toString('utf8');
}

module.exports = {
  initMySQL: initMySQL,
  getSecret: getSecret
};
