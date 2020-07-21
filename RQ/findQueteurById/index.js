'use strict';
const common              = require('./common');
const common_firestore    = require('./common_firestore');
const common_firebase     = require('./common_firebase' );

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();

const queryStr = `
  SELECT  q.id,                                          
          q.email,                                       
          q.first_name,                                  
          q.last_name,                                   
          q.secteur,                                     
          q.nivol,                                       
          q.mobile,                                      
          q.created,                                     
          q.updated,                                     
          q.ul_id,                                       
          q.active,                                      
          q.man,                                         
          q.birthdate,                                   
          q.qr_code_printed,                             
          q.referent_volunteer,                          
          q.anonymization_token,                         
          q.anonymization_date,                          
          u.name        as ul_name,                      
          u.latitude    as ul_latitude,                  
          u.longitude   as ul_longitude,                 
          u.phone       as ul_phone,                     
          u.email       as ul_email,                     
          u.address     as ul_address,                   
          u.postal_code as ul_postal_code,               
          u.city        as ul_city                       
  FROM  queteur  as q,                                     
        ul       as u                                      
  WHERE q.ul_id = u.id                                     
  AND   u.id    = ?                                        
  AND   q.id    = ?                                        
`;


/**
 * Retrieve a Queteur by its ID & UL_ID which are retrieved from firestore from the firebase ID
 * firestore read
 * mysql read
 *
 * **/

// retrieve Queteur Info from it's ID in RCQ DB
exports.findQueteurById = functions.https.onCall( async (data, context) => {
  common_firebase.checkAuthentication(context);
  let mysqlPool = await common.initMySQL('MYSQL_USER_READ');

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  console.log("findQueteurById - uid='"+uid+"', name='"+name+"', email='"+email+"'");

  let queteurData = await common_firestore.getQueteurFromFirestore(uid);

  return new Promise((resolve, reject) => {
    mysqlPool.query(
      queryStr,
      [queteurData.ul_id, queteurData.queteur_id],
      (err, results) => {
        if (err)
        {
          console.error(err);
          reject(err);
        }
        else
        {
          if(results !== undefined && Array.isArray(results) && results.length === 1)
          {
            console.debug("found queteur with id '"+queteurData.queteur_id+"' and ul_id='"+queteurData.ul_id+"' for firestore queteurs(uid='"+uid+"') : " +JSON.stringify(results[0]));
            resolve(JSON.stringify(results[0]));
          }
          else
          {
            let message= "no queteur found with id '"+queteurData.queteur_id+"' and ul_id='"+queteurData.ul_id+"' for firestore queteurs(uid='"+uid+"'), results : " + JSON.stringify(results);
            console.error(message);
            reject(message);
          }
        }
      });
  });
});
