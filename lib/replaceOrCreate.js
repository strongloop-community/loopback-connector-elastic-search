const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function replaceOrCreate(modelName, data, callback) {
  const self = this;
  log('ESConnector.prototype.replaceOrCreate', 'modelName', modelName, 'data', data);

  const idName = self.idName(modelName);
  const id = self.getDocumentId(data[idName]);
  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const document = self.addDefaults(modelName, 'replaceOrCreate');
  document[self.idField] = id;
  document.body = {};
  _.assign(document.body, data);
  document.body.docType = modelName;
  log('ESConnector.prototype.replaceOrCreate', 'document', document);
  self.db.index(
    document
  ).then(
    (response) => {
      log('ESConnector.prototype.replaceOrCreate', 'response', response);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.replaceOrCreate', 'will invoke callback with id:', response._id);
      // eslint-disable-next-line no-underscore-dangle
      callback(null, response._id); // the connector framework expects the id as a return value
    }
  ).catch((err) => {
    log('ESConnector.prototype.replaceOrCreate', err.message);
    return callback(err, null);
  });
}

module.exports.replaceOrCreate = replaceOrCreate;
