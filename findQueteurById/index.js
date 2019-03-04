'use strict';
const {BigQuery} = require('@google-cloud/bigquery');

const bigquery  = new BigQuery();
const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();


const queryStr = [
  'SELECT  q.`id`,                                          ',
  '        q.`email`,                                       ',
  '        q.`first_name`,                                  ',
  '        q.`last_name`,                                   ',
  '        q.`secteur`,                                     ',
  '        q.`nivol`,                                       ',
  '        q.`mobile`,                                      ',
  '        q.`created`,                                     ',
  '        q.`updated`,                                     ',
  '        q.`notes`,                                       ',
  '        q.`ul_id`,                                       ',
  '        q.`active`,                                      ',
  '        q.`man`,                                         ',
  '        q.`birthdate`,                                   ',
  '        q.`qr_code_printed`,                             ',
  '        q.`referent_volunteer`,                          ',
  '        q.`anonymization_token`,                         ',
  '        q.`anonymization_date`,                          ',
  '        u.`name`       as ul_name,                       ',
  '        u.`latitude`   as ul_latitude,                   ',
  '        u.`longitude`  as ul_longitude                   ',
  'FROM `redcrossquest-fr-dev.redcrossquest.queteur` as q,  ',
  '     `redcrossquest-fr-dev.redcrossquest.ul`      as u   ',
  'WHERE q.ul_id = u.id                                     ',
  'AND   q.id    = @id                                      '].join('\n');


function handleError(err){
  if (err && err.name === 'PartialFailureError') {
    if (err.errors && err.errors.length > 0) {
      console.log('Insert errors:');
      err.errors.forEach(err => console.error(err));
    }
  } else {
    console.error('ERROR:', err);
  }
}


// [START findQueteurById]
// retrieve Queteur Info from it's ID in RCQ DB
exports.findQueteurById = functions.https.onCall((data, context) => {
  // [START_EXCLUDE]
  // [START readMessageData]
  // QueteurID
  const queteurId = data.id;
  // [END readMessageData]
  // [START messageHttpsErrors]

  // Checking that the user is authenticated.
  if (!context.auth)
  {
    // Throwing an HttpsError so that the client gets the error details.
    throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
  }

  // Checking attribute.
  if (!(typeof queteurId === 'number') || queteurId <= 0  || queteurId >= 10000000)
  {
    // Throwing an HttpsError so that the client gets the error details.
    throw new functions.https.HttpsError('invalid-argument', 'Parameter id missing or out of range');
  }

  // [END messageHttpsErrors]

  // [START authIntegration]
  // Authentication / user information is automatically added to the request.
  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const picture = context.auth.token.picture || null;
  const email   = context.auth.token.email   || null;

  console.log("uid='"+uid+"', name='"+name+"', picture='"+picture+"', email='"+email+"'");
  // [END authIntegration]

  // [START returnMessageAsync]
  // Saving the new message to the Realtime Database.


  params.id = queteurId;
  console.log("findQueteurById("+queteurId+")");

  const queryObj = {
    query : queryStr,
    params: params
  };

  return bigquery
    .query(queryObj)
    .then((data) => {
      console.log(JSON.stringify(data));
      if(data !== undefined && Array.isArray(data) && data.length === 1)
      {
        return JSON.stringify(data[0]);
      }
      else
      {
        console.log("query returned incorrect number of rows "+ data.length );
        return JSON.stringify([]);
      }
    })
    .catch(err => {
      handleError(err);
      throw new functions.https.HttpsError('unknown', err.message, err);
    });

  // [END_EXCLUDE]
});
// [END messageFunctionTrigger]