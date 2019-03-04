const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = [
  'SELECT  us.`id` as settings_id,       ',
  '        us.`token_benevole`,          ',
  '        us.`token_benevole_1j`,       ',
  '        u.`id` as ul_id,              ',
  '        u.`name`,                     ',
  '        u.`phone`,                    ',
  '        u.`latitude`,                 ',
  '        u.`longitude`,                ',
  '        u.`address`,                  ',
  '        u.`postal_code`,              ',
  '        u.`city`,                     ',
  '        u.`email`                     ',
  'FROM `redcrossquest.ul_settings` us,  ',
  '     `redcrossquest.ul` u             ',
  'WHERE us.ul_id = u.id                 ',
  'AND                                   ',
  '(                                     ',
  '  us.token_benevole    = @token1      ',
  '  OR                                  ',
  '  us.token_benevole_1j = @token2      ',
  ')                                     '].join('\n');


function handleError(err){
  if (err && err.name === 'PartialFailureError') {
    if (err.errors && err.errors.length > 0) {
      console.log('Insert errors:');
      err.errors.forEach(err => console.error(err));
    }
  } else {
    console.error('ERROR:', err);
  }
}



/**
 * Retrieve UL details by its registration token
 *
 * @param {!express:Request} req HTTP request context.
 *        specify a token variable with the token value
 soit par le token d’inscription :
 https://europe-west1-redcrossquest-fr-dev.cloudfunctions.net/findULDetailsByToken?token=be643d0e-bb77-4c71-90b2-cc78a5bd8432

 * @param {!express:Response} res HTTP response context.
 */
exports.findULDetailsByToken = (req, res) => {
  let token  = req.query.token;
  let params = {};

 if(token         !== undefined &&
    typeof token  === 'string'  &&
    token.length  === 36        &&
   (token.match(new RegExp("-", "g")) || []).length === 4)
  {
    params.token1 = token;
    params.token2 = token;
  }
  else
  {
    res.status(500).send("Invalid query parameter, missing token "+(typeof token)+" "+token.length+" "+(token.match(new RegExp("-", "g"))));
  }


  const queryObj = {
    query : queryStr,
    params: params
  };

  return bigquery
    .query(queryObj)
    .then((data) => {
      console.log(JSON.stringify(data));
      if(data !== undefined && Array.isArray(data) && data.length === 1)
      {
        res.status(200).send(JSON.stringify(data[0]));
      }
      else
      {
        console.log("query returned incorrect number of rows "+ JSON.stringify(data) );
        res.status(200).send(JSON.stringify([]));
      }


    })
    .catch(err => {
      handleError(err);
      res.status(500).send('Query Error');
    });
};