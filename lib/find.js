const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function find(modelName, id, done) {
  const self = this;
  log('ESConnector.prototype.find', 'model', modelName, 'id', id);

  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const defaults = self.addDefaults(modelName, 'find');
  self.db.get(_.defaults({
    id: self.getDocumentId(id)
  }, defaults)).then(
    (response) => {
      done(null, self.dataSourceToModel(modelName, response));
    },
    (err) => {
      log('ESConnector.prototype.find', err.message);
      return done(err, null);
    }
  );
}

module.exports.find = find;
