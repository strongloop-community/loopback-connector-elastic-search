let log = null;

const createIndex = (self, params) => {
  log('ESConnector.prototype.setupIndices', 'createIndex()', params);
  return self.db.indices.create(params)
    .then((info) => {
      log('ESConnector.prototype.setupIndices', 'createIndex()', 'self.db.indices.create()', 'response:', info);
      return Promise.resolve();
    }, (err) => {
      if (err.message.indexOf('IndexAlreadyExistsException') !== -1
      || err.message.indexOf('index_already_exists_exception') !== -1) {
        // console.trace('OMG WTF', err);
        log('ESConnector.prototype.setupIndices', 'createIndex()', 'self.db.indices.create()', 'we ate IndexAlreadyExistsException');
        return Promise.resolve();
      }
      log('ESConnector.prototype.setupIndices', 'createIndex()', 'self.db.indices.create()', 'failed:', err);
      return Promise.reject(err);
    });
};

function setupIndex(indexName) {
  const self = this;

  if (!indexName) { // validate input
    return Promise.reject(new Error('missing indexName'));
  }

  const params = {
    index: indexName,
    body: self.searchIndexSettings
  };
  return self.db.indices.exists(params)
    .then((exists) => {
      log('ESConnector.prototype.setupIndices', 'self.db.indices.exists()', 'response:', exists);
      if (!exists) {
        return createIndex(self, params);
      }
      return Promise.resolve();
    }, (err) => {
      log('ESConnector.prototype.setupIndices', 'self.db.indices.exists()', 'failed:', err);
      return Promise.reject(err);
    });
}

module.exports = (dependencies) => {
  log = dependencies
    // eslint-disable-next-line no-console
    ? (dependencies.log || console.log)
    // eslint-disable-next-line no-console
    : console.log;
  return setupIndex;
};
