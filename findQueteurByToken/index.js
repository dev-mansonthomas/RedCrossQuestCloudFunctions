const {BigQuery} = require('@google-cloud/bigquery');
const bigquery   = new BigQuery();

const queryStr = [
  'SELECT  q.`id`,                                          ',
  '        q.`email`,                                       ',
  '        q.`first_name`,                                  ',
  '        q.`last_name`,                                   ',
  '        q.`secteur`,                                     ',
  '        q.`nivol`,                                       ',
  '        q.`mobile`,                                      ',
  '        q.`created`,                                     ',
  '        q.`updated`,                                     ',
  '        q.`notes`,                                       ',
  '        q.`ul_id`,                                       ',
  '        q.`active`,                                      ',
  '        q.`man`,                                         ',
  '        q.`birthdate`,                                   ',
  '        q.`qr_code_printed`,                             ',
  '        q.`referent_volunteer`,                          ',
  '        q.`anonymization_token`,                         ',
  '        q.`anonymization_date`,                          ',
  '        u.`name`       as ul_name,                       ',
  '        u.`latitude`   as ul_latitude,                   ',
  '        u.`longitude`  as ul_longitude                   ',
  'FROM `redcrossquest-fr-dev.redcrossquest.queteur` as q,  ',
  '     `redcrossquest-fr-dev.redcrossquest.ul`      as u   ',
  'WHERE q.ul_id = u.id                                     ',
  'AND q.registration_token = @token                        '].join('\n');


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
 * Retrieve Queteur details by its registration token
 *
 * @param {!express:Request} req HTTP request context.
 *        specify a token variable with the token value
 soit par le token d’inscription :
 https://europe-west1-redcrossquest-fr-dev.cloudfunctions.net/findQueteurByToken?token=be643d0e-bb77-4c71-90b2-cc78a5bd8432

 * @param {!express:Response} res HTTP response context.
 */
exports.findQueteurByToken = (req, res) => {
  let token  = req.query.token;
  let params = {};

 if(token !== undefined  &&
    token.length === 36  &&
   (token.match(new RegExp("-", "g")) || []).length === 4)
  {
    params.token = token;
  }
  else
  {
    res.status(500).send("Invalid query");
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
        res.status(200).send(JSON.stringify([]));
      }


    })
    .catch(err => {
      handleError(err);
      res.status(500).send('Query Error');
    });
};