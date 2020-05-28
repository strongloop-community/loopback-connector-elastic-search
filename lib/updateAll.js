const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');
// CONSTANTS
const SEARCHAFTERKEY = '_search_after';
const TOTALCOUNTKEY = '_total_count';

function updateAll(model, where, data, options, cb) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.updateAll', 'model', model, 'options', options, 'where', where, 'date', data);
  }
  const idName = self.idName(model);
  log('ESConnector.prototype.updateAll', 'idName', idName);

  const defaults = self.addDefaults(model, 'updateAll');

  const reqBody = {
    query: self.buildWhere(model, idName, where).query
  };

  reqBody.script = {
    inline: '',
    params: {}
  };
  _.forEach(data, (value, key) => {
    if (key !== '_id' && key !== idName && key !== SEARCHAFTERKEY && key !== TOTALCOUNTKEY) {
      // default language for inline scripts is painless if ES 5, so this needs the extra params.
      reqBody.script.inline += `ctx._source.${key}=params.${key};`;
      reqBody.script.params[key] = value;
      if (key === 'docType') {
        reqBody.script.params[key] = model;
      }
    }
  });

  const document = _.defaults({
    body: reqBody
  }, defaults);
  log('ESConnector.prototype.updateAll', 'document to update', document);

  self.db.updateByQuery(document)
    .then(({ body }) => {
      log('ESConnector.prototype.updateAll', 'response', body);
      return cb(null, {
        updated: body.updated,
        total: body.total
      });
    }).catch((error) => {
      log('ESConnector.prototype.updateAll', error.message);
      return cb(error);
    });
}

module.exports.updateAll = updateAll;
