const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function destroyAll(modelName, whereClause, cb) {
  const self = this;

  if ((!cb) && _.isFunction(whereClause)) {
    cb = whereClause;
    whereClause = {};
  }
  log('ESConnector.prototype.destroyAll', 'modelName', modelName, 'whereClause', JSON.stringify(whereClause, null, 0));

  const idName = self.idName(modelName);
  const body = {
    query: self.buildWhere(modelName, idName, whereClause).query
  };

  const defaults = self.addDefaults(modelName, 'destroyAll');
  const options = _.defaults({
    body
  }, defaults);
  log('ESConnector.prototype.destroyAll', 'options:', JSON.stringify(options, null, 2));
  self.db.deleteByQuery(options)
    .then((response) => {
      cb(null, response);
    })
    .catch((err) => {
      log('ESConnector.prototype.destroyAll', err.message);
      return cb(err, null);
    });
}

module.exports.destroyAll = destroyAll;
