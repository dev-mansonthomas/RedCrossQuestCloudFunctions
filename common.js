'use strict';
const mysql                        = require('mysql');

const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const secretManagerServiceClient   = new SecretManagerServiceClient();

const connectionName = process.env.INSTANCE_CONNECTION_NAME || null;
const dbUser         = process.env.SQL_USER                 || null;
const dbName         = process.env.SQL_DB_NAME              || null;
const env            = process.env.ENV                      || null;
const country        = process.env.COUNTRY                  || null;
const project        = process.env.PROJECT                  || null;


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
if( env           === null)
{
  throw new Error('env var not defined : ENV'                     );
}
if( country       === null)
{
  throw new Error('env var not defined : COUNTRY'                 );
}
if( project       === null)
{
  throw new Error('env var not defined : PROJECT'                 );
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

//: Promise<T>
async function initMySQL(secretName) {
// Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  if (!module.exports.mysqlPool)
  {
    // Access the secret.
    mysqlConfig.password = await getSecret(secretName);
    module.exports.mysqlPool = mysql.createPool(mysqlConfig);
  }
  return module.exports.mysqlPool;
}

async function getSecret(secretName){
  // Access the secret.
  let secretPath = "projects/"+project+"-"+country+"-"+env+"/secrets/"+secretName+"/versions/latest";
  console.trace("accessing secret with path "+secretPath);
  const [accessResponse] = await secretManagerServiceClient.accessSecretVersion({name: secretPath});
  return accessResponse.payload.data.toString('utf8');
}

function setCors(request, response)
{
  response.set('Access-Control-Allow-Origin', "*");
  response.set('Access-Control-Allow-Methods', 'GET, POST');

  //respond to CORS preflight requests
  if (request.method === 'OPTIONS')
  {
    response.status(204).send('');
    return true;
  }

  return false;
}

module.exports = {
  initMySQL: initMySQL,
  getSecret: getSecret,
  setCors  : setCors  ,
  mysqlPool: mysqlPool
};
