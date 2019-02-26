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



/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 *        Search By QueteurId : specify an 'id' variable with a positive integer value

 https://europe-west1-redcrossquest-fr-dev.cloudfunctions.net/findQueteurById?id=120

 * @param {!express:Response} res HTTP response context.
 */
exports.findQueteurById = (req, res) => {
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
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.ULRankingByWeightCurrentYear = (event, context) => {
  const pubsubMessage = event.data;
  const parsedObject  = JSON.parse(Buffer.from(pubsubMessage, 'base64').toString());

  console.log("Recieved Message : "+JSON.stringify(parsedObject));
  //{ queteur_id: parsedObject.queteur_id, ul_id:parsedObject.ul_id }

  const queryObj = {
    query: queryStr,
    params: {
      ul_id: parsedObject.ul_id
    }
  };

  bigquery
    .query(queryObj)
    .then((data) => {
      console.log(JSON.stringify(data));
      const rows = data[0];
      //rows : [{"amount":367.63,"weight":2399.3,"time_spent_in_minutes":420}]
    })
    .catch(err => {
      handleError(err);
    });

};
