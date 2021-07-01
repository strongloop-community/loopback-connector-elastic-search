const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');
// CONSTANTS
const SEARCHAFTERKEY = '_search_after';
const TOTALCOUNTKEY = '_total_count';

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
  if (document.body[SEARCHAFTERKEY] || document.body[TOTALCOUNTKEY]) {
    document.body[SEARCHAFTERKEY] = undefined;
    document.body[TOTALCOUNTKEY] = undefined;
  }
  log('ESConnector.prototype.replaceOrCreate', 'document', document);
  self.db.index(
    document
  ).then(
    ({ body }) => {
      log('ESConnector.prototype.replaceOrCreate', 'response', body);
      const options = {
        // eslint-disable-next-line no-underscore-dangle
        id: body._id,
        index: self.index
      };
      if (self.mappingType) {
        options.type = self.mappingType;
      }
      return self.db.get(options);
    }
  ).then(({ body }) => {
    const totalCount = typeof body.hits.total === 'object' ? body.hits.total.value : body.hits.total;
    return callback(null, self.dataSourceToModel(modelName, body, idName, totalCount));
  }).catch((error) => {
    log('ESConnector.prototype.replaceOrCreate', error.message);
    return callback(error, null);
  });
}

module.exports.replaceOrCreate = replaceOrCreate;
