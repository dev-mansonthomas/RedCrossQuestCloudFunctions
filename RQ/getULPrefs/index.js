'use strict';
const common              = require('./common');
const common_firebase     = require('./common_firebase' );
const Firestore           = require('@google-cloud/firestore');

const firestoreRCQ = new Firestore ({projectId:process.env.TARGET_PROJECT_ID});
const firestoreRQ  = new Firestore ();

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
exports.getULPrefs = functions.https.onCall(async (data, context) => {

  common_firebase.checkAuthentication(context);

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  common.logDebug("getULPrefs - uid='"+uid+"', name='"+name+"', email='"+email+"'");

  let queteurPromise = await firestoreRQ
    .collection('queteurs')
    .doc(uid)
    .get();

  if(!queteurPromise.exists)
  {
    return {'rq_display_daily_stats':false, 'rq_display_queteur_ranking':'NON', 'rq_autonomous_depart_and_return':false};
  }

  let ul_id = parseInt(queteurPromise.data().ul_id);

  common.logDebug("getULPrefs - ul_id='"+ul_id+"'");

  let ulPrefsPromise = await firestoreRCQ
    .collection('ul_prefs')
    .where('ul_id', '==', ul_id)
    .get();

  common.logDebug("ulPrefsPromise object after query", ulPrefsPromise);

  if (ulPrefsPromise.size === 1)
  {
    common.logDebug("getULPrefs - ul_id='"+ul_id+"'"+JSON.encode(ulPrefsPromise));
    return ulPrefsPromise.docs[1].data();
  }
  else
  {//if not found, we serve the 2019 defaults
    common.logDebug("getULPrefs - ul_id='"+ul_id+"' not found");
    return {'ul_id':0,'rq_display_daily_stats':false, 'rq_display_queteur_ranking':'NON', 'rq_autonomous_depart_and_return':false};
  }
});
