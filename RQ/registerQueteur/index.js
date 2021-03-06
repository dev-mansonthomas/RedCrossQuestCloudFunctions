'use strict';
const common         = require('./common');
const common_firebase= require('./common_firebase' );
const common_mysql   = require('./common_mysql');

const functions      = require('firebase-functions');
const admin          = require('firebase-admin');
const { v4: uuidv4 } = require('uuid');
admin.initializeApp();

const queryStr = `
INSERT INTO \`queteur_registration\`
(\`first_name\`,\`last_name\`,\`man\`,\`birthdate\`,
 \`email\`,\`secteur\`,\`nivol\`,\`mobile\`,\`created\`,
 \`ul_registration_token\`, \`queteur_registration_token\`,
 \`firebase_sign_in_provider\`, \`firebase_uid\`, \`benevole_referent\`)
VALUES
( ?,?,?,?,?,?,?,?,NOW(),?,?,?,?,?)
`;

/**
 * Insert a registration into RCQ MySQL DB
 *
 * Permissions : MySQL Write
 * */
exports.registerQueteur = functions.https.onCall(async (data, context) => {

  common_firebase.checkAuthentication(context);

  const firebaseUID            = context.auth.uid;
  const firebaseEmail          = context.auth.token.email   || null;
  const firebaseSignInProvider = context.auth.token.firebase.sign_in_provider;


  // Initialize the pool lazily, in case SQL access isn't needed for this
  // GCF instance. Doing so minimizes the number of active SQL connections,
  // which helps keep your GCF instances under SQL connection limits.
  let mysqlPool = await common_mysql.initMySQL('MYSQL_USER_WRITE');

  let first_name           = data.first_name           ;
  let last_name            = data.last_name            ;
  let man                  = data.man === 1            ;
  let birthdate            = data.birthdate            ;
  let email                = data.email                ;
  let secteur              = data.secteur              ;
  let nivol                = data.nivol                ;
  let mobile               = data.mobile               ;
  let ul_registration_token= data.ul_registration_token;
  let benevole_referent    = data.benevole_referent    ;
  let queteur_reg_token    = uuidv4();


  return new Promise((resolve, reject) => {
    const queryArgs = [first_name, last_name, man, birthdate, email,
                      secteur, nivol, mobile, ul_registration_token,
                      queteur_reg_token, firebaseSignInProvider,
                      firebaseUID, benevole_referent];

    common.logDebug("Registering new Queteur From",{queryArgs:queryArgs,queryStr:queryStr, context_auth:context.auth});

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
          common.logInfo("registering "+email+" "+queteur_reg_token);
          resolve(JSON.stringify({"queteur_registration_token":queteur_reg_token}));
        }
      });
  }).catch((err) =>{throw new functions.https.HttpsError('unknown', err.message, err);});


  // Close any SQL resources that were declared inside this function.
  // Keep any declared in global scope (e.g. mysqlPool) for later reuse.

});


