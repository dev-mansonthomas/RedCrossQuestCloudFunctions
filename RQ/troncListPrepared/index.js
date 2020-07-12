'use strict';
const common    = require('./common');

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


// [START findQueteurById]
// Return a troncQueteur if exist that is in prepared state (depart_théorique not null and depart null)
// it retrieves queteurId & UL_ID from firestore and then query RCQ MySQL
exports.troncListPrepared = functions.https.onCall(async (data, context) => {
  // [START_EXCLUDE]
  // [START readMessageData]
      //use only the user Id to retrieve it's queteur_id
  // [END readMessageData]
  // [START messageHttpsErrors]

  // Checking that the user is authenticated.
  common.checkAuthentication(context);

  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  let mysqlPool = await common.initMySQL('MYSQL_USER_READ');

  // [END messageHttpsErrors]

  // [START authIntegration]
  // Authentication / user information is automatically added to the request.
  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  console.log("troncListPrepared - uid='"+uid+"', name='"+name+"', email='"+email+"'");
  // [END authIntegration]

  // [START returnMessageAsync]
  // Saving the new message to the Realtime Database.

  let queteurData = await common.getQueteurFromFirestore(uid);

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
  
  // [END_EXCLUDE]
});
// [END messageFunctionTrigger]
