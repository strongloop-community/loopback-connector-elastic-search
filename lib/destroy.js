const log = require('debug')('loopback:connector:elasticsearch');

function destroy(modelName, id, done) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.destroy', 'model', modelName, 'id', id);
  }

  const filter = self.addDefaults(modelName, 'destroy');
  filter[self.idField] = self.getDocumentId(id);
  if (!filter[self.idField]) {
    throw new Error('Document id not setted!');
  }
  self.db.delete(
    filter
  ).then(
    (response) => {
      done(null, response);
    },
    (err) => {
      log('ESConnector.prototype.destroy', err.message);
      return done(err, null);
    }
  );
}

module.exports.destroy = destroy;
