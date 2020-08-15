'use strict';
const functionName   = process.env.FUNCTION_TARGET          || null;
const env            = process.env.ENV                      || null;
const country        = process.env.COUNTRY                  || null;
const project        = process.env.PROJECT                  || null;

if(functionName === null)
{
  throw new Error('env var not defined : FUNCTION_TARGET');
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

const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const secretManagerServiceClient   = new SecretManagerServiceClient();

const { Logging } = require('@google-cloud/logging');
const logging     = new Logging();
const logger      = logging.log(functionName);

async function getSecret(secretName){
  // Access the secret.
  let secretPath = "projects/"+project.toLowerCase()+"-"+country+"-"+env+"/secrets/"+secretName+"/versions/latest";
  logDebug("accessing secret with path "+secretPath);
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

const METADATA = {
  resource: {
    type: 'cloud_function',
    labels: {
      function_name: functionName,
      region: 'europe-west1'
    }
  }
}
;

async function logCritical(message, extraData)
{
  const logData = {
    data: extraData,
    severity: 'critical',
    message: message
  };

  try
  {
    return logger.critical(logger.entry(METADATA, logData));
  }
  catch(exception)
  {//fall back ton console.error
    console.error('error while logging to stack driver', [logData,exception]);
  }
}

async function logError(message, extraData)
{
  const logData = {
    data: extraData,
    severity: 'error',
    message: message
  };
  
  try
  {
    return logger.error(logger.entry(METADATA, logData));
  }
  catch(exception)
  {//fall back ton console.error
    console.error('error while logging to stack driver', [logData,exception]);
  }
}

async function logWarn(message, extraData)
{
  const logData = {
    data: extraData,
    severity: 'warning',
    message: message
  };

  try
  {
    return logger.warn(logger.entry(METADATA, logData));
  }
  catch(exception)
  {//fall back ton console.error
    console.warn('error while logging to stack driver', [logData,exception]);
  }
}


async function logDebug(severity, message, extraData)
{
  const logData = {
    data: extraData,
    severity: 'debug',
    message: message
  };

  try
  {
    return logger.debug(logger.entry(METADATA, logData));
  }
  catch(exception)
  {//fall back ton console.error
    console.debug('error while logging to stack driver', [logData,exception]);
  }
}

async function logInfo(severity, message, extraData)
{
  const logData = {
    data: extraData,
    severity: 'info',
    message: message
  };

  try
  {
    return logger.info(logger.entry(METADATA, logData));
  }
  catch(exception)
  {//fall back ton console.error
    console.info('error while logging to stack driver', [logData,exception]);
  }
}


function handleFirestoreError(err){
  if (err && err.name === 'PartialFailureError') {
    if (err.errors && err.errors.length > 0) {
      common.logError('Insert errors:');
      err.errors.forEach(err => common.logError(err));
    }
  } else {
    common.logError('ERROR:', err);
  }
}

module.exports = {
  getSecret   : getSecret,
  setCors     : setCors  ,
  logError    : logError,
  logWarn     : logWarn,
  logInfo     : logInfo,
  logDebug    : logDebug,
  handleFirestoreError:handleFirestoreError
};
