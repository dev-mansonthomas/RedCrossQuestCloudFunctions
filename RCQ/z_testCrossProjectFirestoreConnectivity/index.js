const Firestore = require('@google-cloud/firestore');
const firestore = new Firestore({ projectId:process.env.TARGET_PROJECT_ID});

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.z_testCrossProjectFirestoreConnectivity = (req, res) => {

  return firestore
  .collection('queteurs')
  .get()
  .then(snap => {
    res.status(200).send(JSON.stringify({'queteur_collection_size':snap.size}));
  })
  .catch(function(error) {
    console.log("Error while counting documents in queteur collection", error);
    res.status(500).send(JSON.stringify(error));
  });
};
