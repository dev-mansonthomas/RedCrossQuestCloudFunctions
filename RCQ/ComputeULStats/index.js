'use strict';
const common              = require('./common');

/***
 * Cloud Scheduler "trigger_ul_update" publish an empty message on "trigger_ul_update"
 * ULTriggerRecompute Cloud Function is listening, on reception it will query the list of active UL 
 * and post a message with this list and an index set to 0
 * 
 * This cloud function receive the message via 'ul_update' topic, make the computation and store it in Firestore,
 * and then republish the same message with the index incremented
 *
 * Requires :
 * roles/cloudsql.client (ou roles/cloudsql.viewer)
 * roles/datastore.user on RQ Firestore
 *
 * data Recieved :
 * const data = {currentIndex:0, uls:results};
 *
 * */
exports.ComputeULStats = async (request, response) => {

  await common.logDebug("ComputeULStats - start", JSON.stringify(request.body));
  const task = request.body;

  if(task.computeType === 'queteurStats')
  {
    await common.logDebug("ULQueteurStatsPerYear - start", task);
    const ULQueteurStatsPerYear              = require('./ULQueteurStatsPerYear.js');
    await ULQueteurStatsPerYear.compute();
    response.status(200).send('ULQueteurStatsPerYear Done');

  }
  else if(task.computeType === 'queteurStats')
  {
    await common.logDebug("ULStatsCurrentYear - start", task);
    const ULStatsCurrentYear              = require('./ULStatsCurrentYear.js');
    await ULStatsCurrentYear.compute();
    response.status(200).send('ULStatsCurrentYear Done');
  }
  else
  {
    await common.logDebug("Wrong value for computeType", task);
    response.status(400).send('Wrong value for computeType');
  }
};
