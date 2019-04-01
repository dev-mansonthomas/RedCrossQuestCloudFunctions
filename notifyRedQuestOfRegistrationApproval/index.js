const Firestore    = require('@google-cloud/firestore');


/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 * Update an existing TroncQueteur with new data
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.notifyRedQuestOfRegistrationApproval = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Received Message for notifyRedQuestOfRegistrationApproval function : "+JSON.stringify(parsedObject));
//Received Message for notifyRedQuestOfRegistrationApproval function : {"id":"4835","email":"na@na.com","first_name":"thomas","last_name":"manson","secteur":1,"nivol":"AFDFD","mobile":"+3374839939","created":{"date":"2019-03-20 19:31:00.000000","timezone_type":3,"timezone":"Europe/Paris"},"updated":null,"notes":"","ul_id":666,"ul_name":"","ul_longitude":2.0,"ul_latitude":48.0,"point_quete_id":null,"point_quete_name":"","depart_theorique":null,"depart":null,"retour":null,"active":true,"man":true,"birthdate":{"date":"1923-12-22 00:00:00.000000","timezone_type":2,"timezone":"Z"},"qr_code_printed":false,"referent_volunteer":0,"referent_volunteer_entity":null,"anonymization_token":"","anonymization_date":null,"ul_registration_token":"61202b60-3cb1-45d4-a4a8-8504233d15f2","queteur_registration_token":"sdf","registration_approved":true,"reject_reason":"","queteur_id":0,"registration_id":1}"



  let updateQueteur = function(documentId, parsedObject)
  {
    Firestore.collection('queteurs')
      .doc(documentId)
      .update(
        {
          registration_approved : parsedObject.registration_approved,
          reject_reason         : parsedObject.reject_reason,
          queteur_id            : parsedObject.queteur_id
      })
  };

  return Firestore.collection('queteurs',
    ref => ref.where('queteur_registration_token', '==', parsedObject.queteur_registration_token))
    .get().then(doc =>
      {

        if(doc.docs.length === 1)
        {
          let document = doc.docs[0];
          console.log("retrieved queteur document to update : "+JSON.stringify(document));
          updateQueteur(document.id, parsedObject);
        }
        else if(doc.docs.length === 0)
        {
          console.log("No document found in 'queteurs' collection for queteur_registration_token "+parsedObject.queteur_registration_token);
        }
        else
        {
          let logString = "Multiple document found with same queteur_registration_token='"+parsedObject.queteur_registration_token+"', documents ids : ";
          for(let i=0; i<doc.docs.length; i++)
          {
            logString+=doc.docs[i].id+", ";
          }
          console.log(logString);
        }
    });
};
