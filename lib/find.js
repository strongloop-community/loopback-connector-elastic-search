const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function find(modelName, id, done) {
  const self = this;
  log('ESConnector.prototype.find', 'model', modelName, 'id', id);

  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }
  const idName = self.idName(modelName);
  const defaults = self.addDefaults(modelName, 'find');
  self.db.get(_.defaults({
    id: self.getDocumentId(id)
  }, defaults)).then(({ body }) => {
    const totalCount = typeof body.hits.total === 'object' ? body.hits.total.value : body.hits.total;
    done(null, self.dataSourceToModel(modelName, body, idName, totalCount));
  }).catch((error) => {
    log('ESConnector.prototype.find', error.message);
    done(error);
  });
}

module.exports.find = find;
