'use strict';
const common_firestore    = require('./common_firestore');

/**
 * Search in RedQuest Firestore for the queteur with it registration UUID
 * Then Update the queteur to set the Approval/Rejection of registration
 * If Approved, set the QueteurId, ULID, Secteur
 *
 * Triggered from a message on a Cloud Pub/Sub topic.
 * Topic : $this->settings['PubSub']['queteur_approval_topic'] (RCQ)
 *
 $messageProperties  = [
 'ulId'          => "".$ulId,
 'uId'           => "".$userId,
 'queteurId'     => "".$queteurEntity->id,
 'registrationId'=> "".$queteurEntity->registration_id
 ];
   Data : QueteurEntity
 *
 * Privileges : roles/datastore.user
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.notifyRQOfRegistApproval = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  common.logDebug("Received Message for notifyRedQuestOfRegistrationApproval function : ",parsedObject);
//Received Message for notifyRedQuestOfRegistrationApproval function : {"id":"4835","email":"na@na.com","first_name":"thomas","last_name":"manson","secteur":1,"nivol":"AFDFD","mobile":"+3374839939","created":{"date":"2019-03-20 19:31:00.000000","timezone_type":3,"timezone":"Europe/Paris"},"updated":null,"notes":"","ul_id":666,"ul_name":"","ul_longitude":2.0,"ul_latitude":48.0,"point_quete_id":null,"point_quete_name":"","depart_theorique":null,"depart":null,"retour":null,"active":true,"man":true,"birthdate":{"date":"1923-12-22 00:00:00.000000","timezone_type":2,"timezone":"Z"},"qr_code_printed":false,"referent_volunteer":0,"referent_volunteer_entity":null,"anonymization_token":"","anonymization_date":null,"ul_registration_token":"61202b60-3cb1-45d4-a4a8-8504233d15f2","queteur_registration_token":"sdf","registration_approved":true,"reject_reason":"","queteur_id":0,"registration_id":1}"

  let updateQueteur = function(documentId, parsedObject)
  {
    common_firestore.firestore.collection('queteurs')
      .doc(documentId)
      .update(
        {
          registration_approved : parsedObject.registration_approved,
          reject_reason         : parsedObject.reject_reason,
          queteur_id            : parsedObject.id,
          ul_id                 : parsedObject.ul_id,
          secteur               : parsedObject.secteur
      })
  };

  return common_firestore.firestore.collection('queteurs').where('queteur_registration_token', '==', parsedObject.queteur_registration_token)
  .get()
  .then(doc =>
      {

        if(doc.docs.length === 1)
        {
          let document = doc.docs[0];
          common.logDebug("retrieved queteur document to update : "+JSON.stringify(document));
          updateQueteur(document.id, parsedObject);
        }
        else if(doc.docs.length === 0)
        {
          common.logInfo("No document found in 'queteurs' collection for queteur_registration_token "+parsedObject.queteur_registration_token);
        }
        else
        {
          let logString = "Multiple document found with same queteur_registration_token='"+parsedObject.queteur_registration_token+"', documents ids : ";
          for(let i=0; i<doc.docs.length; i++)
          {
            logString+=doc.docs[i].id+", ";
          }
          common.logWarn(logString);
        }
    })
  .catch(function(error) {
    common.logError("Error while searching for queteur with queteur_registration_token",
      {parsedObject:parsedObject, error: error});
  });
};
