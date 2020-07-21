'use strict';
const functions                    = require('firebase-functions');

function checkAuthentication(context)
{
  if (!context.auth)
  {
    // Throwing an HttpsError so that the client gets the error details.
    throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
  }
}



module.exports = {
  checkAuthentication : checkAuthentication,
};
