'use strict';
const common    = require('./common');

const functions = require('firebase-functions');
const admin     = require('firebase-admin');
admin.initializeApp();

const queryTmpl = `
  UPDATE tronc_queteur     
  SET    column     = ?    
  WHERE  id         = ?    
  AND    ul_id      = ?    
  AND    queteur_id = ?
  AND    deleted    = false
`;


/**
 update troncQueteur with date depart or retour in RCQ MySQL
 it retrieves queteurId & UL_ID from firestore and then query RCQ MySQL
 * */
exports.troncSetDepartOrRetour = functions.https.onCall(async (data, context) => {

  common.checkAuthentication(context);
  let mysqlPool = await common.initMySQL('MYSQL_USER_WRITE');

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  console.log("troncSetDepartOrRetour - uid='"+uid+"', name='"+name+"', email='"+email+"'");

  let isDepart = data.isDepart;
  let date     = data.date;
  let tqId     = data.tqId;

  let queryStr = queryTmpl.replace('column', isDepart?'depart':'retour');

  let queteurData = await common.getQueteurFromFirestore(uid);

  return new Promise((resolve, reject) => {
    mysqlPool.query(
      queryStr,
      [date, tqId, queteurData.ul_id, queteurData.queteur_id],
      (err, results) => {
        if (err)
        {
          console.error(err);
          reject(err);
        }
        else
        {
          if(results !== undefined && results.affectedRows === 1)
          {
            console.debug(`Update Depart/Tronc (isDepart='${isDepart}') tqId='${tqId}', ulId='${queteurData.ul_id}' queteurId='${queteurData.queteur_id}' did update one row with query ${queryStr}`);
            resolve(JSON.stringify({success:true}));
          }
          else
          {

            console.error(`Update Depart/Tronc (isDepart='${isDepart}') tqId='${tqId}', ulId='${queteurData.ul_id}' queteurId='${queteurData.queteur_id}' did not update the correct number of row (ie : 1) with query '${queryStr}' affectedRows: ${results.affectedRows} `+JSON.stringify([date, tqId, ulId, queteurId]));
            reject(JSON.stringify({success:false, message:`incorrect number of rows updated: ${results.affectedRows}`}));
          }
        }
      });
  });
});
