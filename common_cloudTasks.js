'use strict';
const common            = require('./common');
const {v2beta3}         = require('@google-cloud/tasks');
const cloudTasksClient  = new v2beta3.CloudTasksClient();

let projectName = common.getProjectName();
let location    = "europe-west3";
let queue       = "compute-stats-on-mysql";
const parent    = cloudTasksClient.queuePath(projectName, location, queue);

async function createTask(url, serviceAccount, data)
{
  const dataBuffer  = Buffer.from(JSON.stringify(data));

  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url,
      oidcToken: {
        serviceAccountEmail: serviceAccount,
      },
      headers: {
        'Content-Type': 'application/json',
      },
      dataBuffer,
    },
  };


  try
  {
    // Send create task request.
    const [response] = await cloudTasksClient.createTask({parent, task});
    common.logDebug(`Created task ${response.name}`, {parent:parent,task:task, response:response});
    return response;
  }
  catch (error)
  {
    // Construct error for Stackdriver Error Reporting
    console.error(Error(error.message));
  }
}

module.exports = {
  createTask : createTask,
  cloudTasksClient:cloudTasksClient
};
