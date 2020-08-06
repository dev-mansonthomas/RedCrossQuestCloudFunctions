'use strict';
const common              = require('./common');
const common_firestore    = require('./common_firestore');
const common_firebase     = require('./common_firebase' );
const common_mysql        = require('./common_mysql');

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

  common_firebase.checkAuthentication(context);
  let mysqlPool = await common_mysql.initMySQL('MYSQL_USER_WRITE');

  const uid     = context.auth.uid;
  const name    = context.auth.token.name    || null;
  const email   = context.auth.token.email   || null;

  common.logDebug("troncSetDepartOrRetour - uid='"+uid+"', name='"+name+"', email='"+email+"'");

  let isDepart = data.isDepart;
  let date     = data.date;
  let tqId     = data.tqId;

  let queryStr = queryTmpl.replace('column', isDepart?'depart':'retour');

  let queteurData = await common_firestore.getQueteurFromFirestore(uid);

  return new Promise((resolve, reject) => {
    const queryArgs = [date, tqId, queteurData.ul_id, queteurData.queteur_id];
    mysqlPool.query(
      queryStr,
      queryArgs,
      (err, results) => {
        if (err)
        {
          common.logError("error while running query ", {queryStr:queryStr, mysqlArgs:queryArgs, exception:err});
          reject(err);
        }
        else
        {
          if(results !== undefined && results.affectedRows === 1)
          {
            common.logDebug(`Update Depart/Tronc (isDepart='${isDepart}') tqId='${tqId}', ulId='${queteurData.ul_id}' queteurId='${queteurData.queteur_id}' did update one row with query ${queryStr}`);
            resolve(JSON.stringify({success:true}));
          }
          else
          {

            common.logError(`Update Depart/Tronc (isDepart='${isDepart}') tqId='${tqId}', ulId='${queteurData.ul_id}' queteurId='${queteurData.queteur_id}' did not update the correct number of row (ie : 1) with query '${queryStr}' affectedRows: ${results.affectedRows} `, {date:date, troncQueteurId:tqId, ulId:queteurData.ul_id, queteurId:queteurData.queteur_id});
            reject(JSON.stringify({success:false, message:`incorrect number of rows updated: ${results.affectedRows}`}));
          }
        }
      });
  });
});
