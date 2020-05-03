const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');
// CONSTANTS
const SEARCHAFTERKEY = '_search_after';

function updateAttributes(modelName, id, data, callback) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.updateAttributes', 'modelName', modelName, 'id', id, 'data', data);
  }
  const idName = self.idName(modelName);
  log('ESConnector.prototype.updateAttributes', 'idName', idName);

  const defaults = self.addDefaults(modelName, 'updateAll');

  const reqBody = {
    query: self.buildWhere(modelName, idName, {
      _id: id
    }).query
  };

  reqBody.script = {
    inline: '',
    params: {}
  };
  _.forEach(data, (value, key) => {
    if (key !== '_id' && key !== idName && key !== SEARCHAFTERKEY) {
      // default language for inline scripts is painless if ES 5, so this needs the extra params.
      reqBody.script.inline += `ctx._source.${key}=params.${key};`;
      reqBody.script.params[key] = value;
      if (key === 'docType') {
        reqBody.script.params[key] = modelName;
      }
    }
  });

  const document = _.defaults({
    body: reqBody
  }, defaults);
  log('ESConnector.prototype.updateAttributes', 'document to update', document);

  self.db.updateByQuery(document)
    .then(({ body }) => {
      log('ESConnector.prototype.updateAttributes', 'response', body);
      return callback(null, {
        updated: body.updated,
        total: body.total
      });
    }).catch((error) => {
      log('ESConnector.prototype.updateAttributes', error.message);
      return callback(error);
    });
}

module.exports.updateAttributes = updateAttributes;
