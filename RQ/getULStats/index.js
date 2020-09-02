'use strict';
const common              = require('./common');
const common_firebase     = require('./common_firebase' );
const common_firestore     = require('./common_firestore' );

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();

// [START findQueteurById]
/**
 * Retrieve the RCQ/firestore/ULPreferences
 *
 * Permissions :
 * firestore read on RCQ
 * logwriter
 * */
exports.getULStats = functions.https.onCall(async (data, context) => {

  common_firebase.checkAuthentication(context);

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  common.logDebug("getULStats - uid='"+uid+"', name='"+name+"', email='"+email+"'");

  let queteur = null;

  try
  {
    queteur = await common_firestore.getQueteurFromFirestore(uid);
  }
  catch(e)
  {//
    common.logError("error while getting queteur with uid='"+uid+"', name='"+name+"', email='"+email+"'", e);
    return null;
  }

  let ul_id = parseInt(queteur.ul_id);

  common.logDebug("getULStats - ul_id='"+ul_id+"'");

  let ulStatsPromise = await common_firestore.firestore
    .collection('ul_stats_current_year')
    .where('ul_id', '==', ul_id)
    .get();

  common.logDebug("ulStatsPromise object after query on collection ul_stats_current_year", ulStatsPromise);

  if (ulStatsPromise.size === 1)
  {
    common.logDebug("getULStats - ul_id='"+ul_id+"'"+JSON.stringify(ulStatsPromise.docs[0].data()));
    return ulStatsPromise.docs[0].data();
  }
  else
  {//if not found, we serve the 2019 defaults
    common.logDebug("getULStats - ul_id='"+ul_id+"' not found or more than one result");
    return null;
  }
});
