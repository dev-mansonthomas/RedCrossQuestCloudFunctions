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

  common.log('TRACE', 'troncListPrepared starting', {uid:uid, name:name, email:email});
  common.log('INFO', 'troncListPrepared starting', {uid:uid, name:name, email:email});
  common.log('WARN', 'troncListPrepared starting', {uid:uid, name:name, email:email});
  common.log('ERROR', 'troncListPrepared starting', {uid:uid, name:name, email:email});
  common.log('CRITICAL', 'troncListPrepared starting', {uid:uid, name:name, email:email});

  let queteurData = await common_firestore.getQueteurFromFirestore(uid);

  return new Promise((resolve, reject) => {
    mysqlPool.query(
      queryStr,
      [queteurData.ul_id, queteurData.queteur_id],
      (err, results) => {
        if (err)
        {
          common.log('ERROR', 'troncListPrepared error while querying MySQL', {queryStr:queryStr, mysqlArgs:[queteurData.ul_id, queteurData.queteur_id], exception:err});
          reject(err);
        }
        else
        {
          if(results !== undefined && Array.isArray(results) && results.length >= 1)
          {
            common.log('INFO', 'troncListPrepared - found preparedTronc for queteur ', {uid:uid, name:name, email:email, queteurId:queteurData.queteur_id, queteurUlId:queteurData.ul_id, results:JSON.stringify(results)});
            resolve(JSON.stringify(results));
          }
          else
          {
            common.log('ERROR', 'troncListPrepared - no preparedTronc found for queteur ', {uid:uid, name:name, email:email, queteurId:queteurData.queteur_id, queteurUlId:queteurData.ul_id, results:JSON.stringify(results)});
            resolve(JSON.stringify([]));
          }
        }
      });
  });
});
