const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function exists(modelName, id, done) {
  const self = this;
  log('ESConnector.prototype.exists', 'model', modelName, 'id', id);

  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const defaults = self.addDefaults(modelName, 'exists');
  self.db.exists(_.defaults({
    id: self.getDocumentId(id)
  }, defaults)).then(({ body }) => {
    done(null, body);
  }).catch((error) => {
    log('ESConnector.prototype.exists', error.message);
    done(error);
  });
}

module.exports.exists = exists;
