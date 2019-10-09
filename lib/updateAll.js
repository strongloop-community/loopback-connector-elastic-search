const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function updateAll(model, where, data, options, cb) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.updateAll', 'model', model, 'options', options, 'where', where, 'date', data);
  }
  const idName = self.idName(model);
  log('ESConnector.prototype.updateAll', 'idName', idName);

  const defaults = self.addDefaults(model, 'updateAll');

  const body = {
    query: self.buildWhere(model, idName, where).query
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
        body.script.params[key] = model;
      }
    }
  });

  const document = _.defaults({
    body
  }, defaults);
  log('ESConnector.prototype.updateAll', 'document to update', document);

  self.db.updateByQuery(document)
    .then((response) => {
      log('ESConnector.prototype.updateAll', 'response', response);
      return cb(null, {
        updated: response.updated,
        total: response.total
      });
    }, (err) => {
      log('ESConnector.prototype.updateAll', err.message);
      return cb(err, null);
    });
}

module.exports.updateAll = updateAll;
