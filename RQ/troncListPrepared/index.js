'use strict';
const common              = require('./common');
const common_firestore    = require('./common_firestore');
const common_firebase     = require('./common_firebase' );

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();

const queryStr = `
  SELECT tq.id as tronc_queteur_id,    
         tq.queteur_id,                
         tq.point_quete_id,            
         tq.tronc_id,                  
         tq.depart_theorique,          
         tq.depart,                    
         pq.name,                      
         pq.latitude,                  
         pq.longitude,                 
         pq.address,                   
         pq.postal_code,               
         pq.city,                      
         pq.advice,                    
         pq.localization               
  FROM   tronc_queteur   tq,           
          queteur         q,           
          point_quete    pq            
  WHERE tq.queteur_id       = q.id     
  AND   tq.point_quete_id   = pq.id    
  AND    q.active           = 1        
  AND   tq.deleted          = false    
  AND   tq.ul_id            = ?        
  AND   tq.queteur_id       = ?        
  AND   tq.depart_theorique is not null
  AND                                  
  (                                    
     tq.depart              is null    
     OR                                
     tq.retour              is null    
  )
`;



exports.troncListPrepared = functions.https.onCall(async (data, context) => {

  common_firebase.checkAuthentication(context);
  let mysqlPool = await common.initMySQL('MYSQL_USER_READ');

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  console.log("troncListPrepared - uid='"+uid+"', name='"+name+"', email='"+email+"'");

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
          if(results !== undefined && Array.isArray(results) && results.length >= 1)
          {
            console.debug("found preparedTronc for queteurwith id '"+queteurId+"' and ul_id='"+ulId+"' for firestore queteurs(uid='"+uid+"') : " +JSON.stringify(results));
            resolve(JSON.stringify(results));
          }
          else
          {
            console.error("no preparedTronc found for queteur with id '"+queteurId+"' and ul_id='"+ulId+"' for firestore queteurs(uid='"+uid+"') "+JSON.stringify(results));
            resolve(JSON.stringify([]));
          }
        }
      });
  });
});
