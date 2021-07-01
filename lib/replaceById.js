const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');
// CONSTANTS
const SEARCHAFTERKEY = '_search_after';
const TOTALCOUNTKEY = '_total_count';

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
  document.body['docType.keyword']  = modelName;
  if (Object.prototype.hasOwnProperty.call(modelProperties, idName)) {
    document.body[idName] = id;
  }
  if (document.body[SEARCHAFTERKEY] || document.body[TOTALCOUNTKEY]) {
    document.body[SEARCHAFTERKEY] = undefined;
    document.body[TOTALCOUNTKEY] = undefined;
  }
  log('ESConnector.prototype.replaceById', 'document', document);
  self.db.index(
    document
  ).then(
    ({ body }) => {
      log('ESConnector.prototype.replaceById', 'response', body);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.replaceById', 'will invoke callback with id:', body._id);
      // eslint-disable-next-line no-underscore-dangle
      callback(null, body._id); // the connector framework expects the id as a return value
    }
  ).catch((error) => {
    log('ESConnector.prototype.replaceById', error.message);
    callback(error);
  });
}

module.exports.replaceById = replaceById;
