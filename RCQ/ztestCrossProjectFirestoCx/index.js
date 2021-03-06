const Firestore = require('@google-cloud/firestore');
const firestore = new Firestore({projectId:process.env.TARGET_PROJECT_ID});

/**
 * Responds to any HTTP request.
 *
 * Check if RQ Firestore is reachable from RCQ
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.ztestCrossProjectFirestoCx = (req, res) => {

  return firestore
  .collection('queteurs')
  .get()
  .then(snap => {
    res.status(200).send(JSON.stringify({'queteur_collection_size':snap.size}));
  })
  .catch(function(error) {
    common.logError("Error while counting documents in queteur collection", error);
    res.status(500).send(JSON.stringify(error));
  });
};
