const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function updateAttributes(modelName, id, data, callback) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.updateAttributes', 'modelName', modelName, 'id', id, 'data', data);
  }
  const idName = self.idName(modelName);
  log('ESConnector.prototype.updateAttributes', 'idName', idName);

  const defaults = self.addDefaults(modelName, 'updateAll');

  const body = {
    query: self.buildWhere(modelName, idName, {
      _id: id
    }).query
  };

  body.script = {
    inline: '',
    params: {}
  };
  _.forEach(data, (value, key) => {
    if (key !== '_id' || key !== idName) {
      // default language for inline scripts is painless if ES 5, so this needs the extra params.
      body.script.inline += `ctx._source.${key}=params.${key};`;
      body.script.params[key] = value;
      if (key === 'docType') {
        body.script.params[key] = modelName;
      }
    }
  });

  const document = _.defaults({
    body
  }, defaults);
  log('ESConnector.prototype.updateAttributes', 'document to update', document);

  self.db.updateByQuery(document)
    .then((response) => {
      log('ESConnector.prototype.updateAttributes', 'response', response);
      return callback(null, {
        updated: response.updated,
        total: response.total
      });
    }, (err) => {
      log('ESConnector.prototype.updateAttributes', err.message);
      return callback(err, null);
    });
}

module.exports.updateAttributes = updateAttributes;
