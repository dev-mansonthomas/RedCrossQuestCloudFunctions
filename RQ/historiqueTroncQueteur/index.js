'use strict';
const common    = require('./common');
const moment    = require('moment');

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();

const queryStr = `
  SELECT                                
    tq.id,                              
    tq.point_quete_id,                  
    pq.name as point_quete,             
    tq.tronc_id,                        
    tq.depart_theorique,                
    tq.depart,                          
    tq.retour,                          
    tq.comptage,                        
    (tq.euro2*2     +                   
     tq.euro1*1     +                   
     tq.cents50*0.5 +                   
     tq.cents20*0.2 +                   
     tq.cents10*0.1 +                   
     tq.cents5*0.05 +                   
     tq.cents2*0.02 +                   
     tq.cent1*0.01  +                   
     tq.euro5*5     +                   
     tq.euro10*10   +                   
     tq.euro20*20   +                   
     tq.euro50*50   +                   
     tq.euro100*100 +                   
     tq.euro200*200 +                   
     tq.euro500*500 +                   
     tq.don_cheque  +                   
     tq.don_creditcard                  
    ) as amount,                        
    ( tq.euro500 * 1.1  +               
     tq.euro200  * 1.1  +               
     tq.euro100  * 1    +               
     tq.euro50   * 0.9  +               
     tq.euro20   * 0.8  +               
     tq.euro10   * 0.7  +               
     tq.euro5    * 0.6  +               
     tq.euro2    * 8.5  +               
     tq.euro1    * 7.5  +               
     tq.cents50  * 7.8  +               
     tq.cents20  * 5.74 +               
     tq.cents10  * 4.1  +               
     tq.cents5   * 3.92 +               
     tq.cents2   * 3.06 +               
     tq.cent1    * 2.3                  
    ) as weight,                        
    (timestampdiff(                     
      second,                           
      depart,                           
      retour))/3600                     
      as time_spent_in_hours,           
    tq.don_creditcard,                  
    tq.don_cheque                       
    FROM tronc_queteur as tq,           
         point_quete   as pq,           
         queteur       as q             
  WHERE  q.id             = ?           
  AND   tq.queteur_id     = q.id        
  AND    q.ul_id          = ?           
  AND   tq.point_quete_id = pq.id       
  AND   YEAR(tq.depart)   = YEAR(NOW()) 
  AND   tq.comptage       IS NOT NULL   
  AND   tq.deleted        = 0           
  ORDER BY tq.id DESC;                  
`;


// [START findQueteurById]
/**
 * Retrieve the tronc_queteurs list from MySQL with a cache in firestore with TTL of 5minutes,
 * unless there's no rows, and the check in DB is allowed every minute in this case
 *
 * Permissions :
 * firestore read/write
 * MySQL read
 * */
exports.historiqueTroncQueteur = functions.https.onCall(async (data, context) => {

  common.checkAuthentication(context);
  let mysqlPool = await common.initMySQL('MYSQL_USER_READ');

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  console.log("historiqueTroncQueteur - uid='"+uid+"', name='"+name+"', email='"+email+"'");

  let queteurData = await common.getQueteurFromFirestore(uid);

  console.log("queteurData:"+JSON.stringify(queteurData));
  
  return new Promise((resolve, reject) => {

    if(
      queteurData.historiqueTQLastUpdate                                     != null  &&
      moment().diff(queteurData.historiqueTQLastUpdate, 'seconds') <= 5*60  &&
      Array.isArray(queteurData.historiqueTQ)                                         &&
      (
        queteurData.historiqueTQ.length > 0                                           ||
        moment().diff(queteurData.historiqueTQLastUpdate, 'seconds') <= 60
      )
    )
    {// if the data is fresh enough, return the cached data
      console.log("historique tronc_queteur retrieved from cache "+JSON.stringify(queteurData));
      resolve(queteurData.historiqueTQ);
    }
    else
    {// cache miss or data too old
      console.log("historique tronc_queteur cache miss for queteur id "+queteurData.queteur_id);

      mysqlPool.query(
        queryStr,
        [queteurData.queteur_id, queteurData.ul_id],
        (err, results) => {
          if (err)
          {
            console.error(err);
            reject(err);
          }
          else
          {
            let myResults = JSON.parse(JSON.stringify(results));
            common.updateQueteurFromFirestore
              (
                uid,
                {
                  'historiqueTQ':myResults,
                  'historiqueTQLastUpdate': JSON.parse(JSON.stringify(new Date()))
                }
              ).then
            (
              ()=>{resolve(myResults);}
            ).catch(updateError=>{
              console.error(updateError);
              reject(updateError);
            });
          }
        }
      );
    }
  });
});
