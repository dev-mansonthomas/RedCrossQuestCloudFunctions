'use strict';
const common    = require('./common');
const common_mysql        = require('./common_mysql');

exports.ztestCrossProjectSQLCx = async (req, res) => {

  let mysqlPool = await common_mysql.initMySQL('MYSQL_USER_READ');

  const queryStr = `
select count(1) as nb_tq
from tronc_queteur
`;

  return new Promise((resolve, reject) => {
    mysqlPool.query(
      queryStr,
      [],
      (err, results) => {
        if (err)
        {
          common.logError("error while running query ", {queryStr:queryStr, mysqlArgs:[], exception:err});
          res.status(500).send(JSON.stringify(err));
          reject(err);
        }
        else
        {
          common.logDebug("Number "+JSON.stringify(results));
          res.status(200).send(JSON.stringify(results));
          resolve(JSON.stringify(results));
        }
      });
  }).catch((err) =>{
    common.logError(err.message, JSON.stringify(err));
    res.status(500).send(JSON.stringify(err));
  });

};
