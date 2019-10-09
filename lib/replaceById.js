const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function replaceById(modelName, id, data, options, callback) {
  const self = this;
  log('ESConnector.prototype.replaceById', 'modelName', modelName, 'id', id, 'data', data);

  const idName = self.idName(modelName);
  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  // eslint-disable-next-line no-underscore-dangle
  const modelProperties = this._models[modelName].properties;

  const document = self.addDefaults(modelName, 'replaceById');
  document[self.idField] = self.getDocumentId(id);
  document.body = {};
  _.assign(document.body, data);
  document.body.docType = modelName;
  if (Object.prototype.hasOwnProperty.call(modelProperties, idName)) {
    document.body[idName] = id;
  }
  log('ESConnector.prototype.replaceById', 'document', document);
  self.db.index(
    document
  ).then(
    (response) => {
      log('ESConnector.prototype.replaceById', 'response', response);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.replaceById', 'will invoke callback with id:', response._id);
      // eslint-disable-next-line no-underscore-dangle
      callback(null, response._id); // the connector framework expects the id as a return value
    }
  ).catch((err) => {
    log('ESConnector.prototype.replaceById', err.message);
    return callback(err, null);
  });
}

module.exports.replaceById = replaceById;
