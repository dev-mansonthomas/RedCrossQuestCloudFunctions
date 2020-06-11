'use strict';
const mysql     = require('mysql');
require('common');

const queryStr = `
  SELECT  us.id as settings_id,       
          us.token_benevole,          
          us.token_benevole_1j,       
          u.id as ul_id,              
          u.name,                     
          u.phone,                    
          u.latitude,                 
          u.longitude,                
          u.address,                  
          u.postal_code,              
          u.city,                     
          u.email                     
  FROM ul_settings us,                  
       ul u                             
  WHERE us.ul_id = u.id                 
  AND                                   
  (                                     
    us.token_benevole    = ?            
    OR                                  
    us.token_benevole_1j = ?            
  )                                     
`;


/**
 * Retrieve UL details by its registration token  from RCQ MySQL
 *
 * MySQL read
 *
 * @param {!express:Request} req HTTP request context.
 *        specify a token variable with the token value
 soit par le token d’inscription :
 https://europe-west1-rq-fr-dev.cloudfunctions.net/findULDetailsByToken?token=be643d0e-bb77-4c71-90b2-cc78a5bd8432

 * @param {!express:Response} res HTTP response context.
 */
exports.findULDetailsByToken = (req, res) => {
  res.set('Access-Control-Allow-Origin', "*");
  res.set('Access-Control-Allow-Methods', 'GET, POST');
  let token  = req.query.token;
  let params = {};

 if(
   !( token         !== undefined &&
      typeof token  === 'string'  &&
      token.length  === 36        &&
     (token.match(new RegExp("-", "g")) || []).length === 4
    ))
  {
    res.status(500).send("Invalid query parameter, missing token "+(typeof token)+" "+token.length+" "+(token.match(new RegExp("-", "g"))));
  }

  return new Promise((resolve, reject) => {
    mysqlPool.query(queryStr, [token, token],
      (err, results) => {
        if (err)
        {
          console.error(err);
          res.status(500).send(JSON.stringify(err));
          reject(err);
        }
        else
        {
          if(results !== undefined && Array.isArray(results) && results.length === 1)
          {
            res.status(200).send(JSON.stringify(results[0]));
            resolve(JSON.stringify(results[0]));
          }
          else
          {
            console.log("query returned incorrect number of rows with token '"+token+"'"+ JSON.stringify(results) );
            res.status(200).send(JSON.stringify([]));
          }

        }
      });
  }).catch((err) =>{
    console.info(err.message, JSON.stringify(err));
    res.status(500).send(JSON.stringify(err));
  });
};
