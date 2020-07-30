'use strict';
const Firestore = require('@google-cloud/firestore');
// if  process.env.TARGET_PROJECT_ID is defined, it's RCQ CF that tries to reach RQ Firestore
// otherwise, it's RQ reaching its own firestore
const firestore = process.env.TARGET_PROJECT_ID ? new Firestore ({projectId:process.env.TARGET_PROJECT_ID}) : new Firestore ();
firestore.settings({timestampsInSnapshots: true});

async function getQueteurFromFirestore(uid)
{
  let queteurPromise = await firestore
    .collection('queteurs')
    .doc(uid)
    .get();

  if (queteurPromise.exists)
  {
    return queteurPromise.data();
  }
  else
  {
    throw new functions.https.HttpsError('not-found', "queteur with uid='"+uid+" not found");
  }
}


async function updateQueteurFromFirestore(uid, data)
{
  return firestore
    .collection('queteurs')
    .doc(uid)
    .update(data);
}

module.exports = {
  getQueteurFromFirestore : getQueteurFromFirestore,
  updateQueteurFromFirestore:updateQueteurFromFirestore,
  firestore:firestore
};
