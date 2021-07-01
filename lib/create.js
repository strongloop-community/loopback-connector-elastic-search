const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');
// CONSTANTS
const SEARCHAFTERKEY = '_search_after';
const TOTALCOUNTKEY = '_total_count';

function create(model, data, done) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.create', model, data);
  }

  const idValue = self.getIdValue(model, data);
  const idName = self.idName(model);
  log('ESConnector.prototype.create', 'idName', idName, 'idValue', idValue);
  /* TODO: If model has custom id with generated false and
    if Id field is not prepopulated then create should fail.
  */
  /* TODO: If model Id is not string and generated is true then
    create should fail because the auto generated es id is of type string and we cannot change it.
  */
  const document = self.addDefaults(model, 'create');
  document[self.idField] = self.getDocumentId(idValue);
  document.body = {};
  _.assign(document.body, data);
  log('ESConnector.prototype.create', 'document', document);
  let method = 'create';
  if (!document[self.idField]) {
    method = 'index'; // if there is no/empty id field, we must use the index method to create it (API 5.0)
  }
  document.body['docType.keyword']= model;
  if (document.body[SEARCHAFTERKEY] || document.body[TOTALCOUNTKEY]) {
    document.body[SEARCHAFTERKEY] = undefined;
    document.body[TOTALCOUNTKEY] = undefined;
  }
  self.db[method](
    document
  ).then(
    ({ body }) => {
      log('ESConnector.prototype.create', 'response', body);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.create', 'will invoke callback with id:', body._id);
      // eslint-disable-next-line no-underscore-dangle
      done(null, body._id); // the connector framework expects the id as a return value
    }
  ).catch((error) => {
    log('ESConnector.prototype.create', error.message);
    return done(error, null);
  });
}

module.exports.create = create;
