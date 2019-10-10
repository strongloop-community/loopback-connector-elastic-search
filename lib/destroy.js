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
  ).then(({ body }) => {
    done(null, body);
  }).catch((error) => {
    log('ESConnector.prototype.destroy', error.message);
    done(error, null);
  });
}

module.exports.destroy = destroy;
