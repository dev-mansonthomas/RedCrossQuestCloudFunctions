const {BigQuery} = require('@google-cloud/bigquery');
const admin      = require("firebase-admin");
const cors       = require("cors");

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
  'AND   q.id    = @id                                      '].join('\n');


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



findQueteurByIdImpl=(req, res, decoded)=> {

  console.log("Authenticated Request : "+JSON.stringify(decoded));

  let queteurId = req.query.id ;
  let params = {};

  if(queteurId !== undefined && queteurId.length < 10 && queteurId.match(/^\d+$/))
  {
    queteurId = parseInt(queteurId);
    params.id = queteurId;
  }
  else
  {
    res.status(500).send("Invalid query");
  }

  console.log("findQueteurById("+queteurId+")");

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



/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 *        Search By QueteurId : specify an 'id' variable with a positive integer value

 https://europe-west1-redcrossquest-fr-dev.cloudfunctions.net/findQueteurById?id=120

 * @param {!express:Response} res HTTP response context.
 */
exports.findQueteurById = (req, res) => {

  console.log("findQueteurById called - before cors()");

  cors(req, res, () => {
    console.log("findQueteurById called");
    const tokenId = req.get('Authorization').split('Bearer ')[1];
    console.log("findQueteurById called with tokenId: "+tokenId);

    return admin.auth().verifyIdToken(tokenId)
      .then ((decoded) => res.status(200).send(decoded))
      .catch((err   )  => res.status(401).send(err));
  });
};