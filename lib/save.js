const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');
// CONSTANTS
const SEARCHAFTERKEY = '_search_after';
const TOTALCOUNTKEY = '_total_count';

// eslint-disable-next-line consistent-return
function save(model, data, done) {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.save ', 'model', model, 'data', data);
  }

  const idName = self.idName(model);
  const defaults = self.addDefaults(model, 'save');
  const id = self.getDocumentId(data[idName]);

  if (id === undefined || id === null) {
    return done('Document id not setted!', null);
  }
  data.docType = model;
  if (data[SEARCHAFTERKEY] || data[TOTALCOUNTKEY]) {
    data[SEARCHAFTERKEY] = undefined;
    data[TOTALCOUNTKEY] = undefined;
  }
  self.db.update(_.defaults({
    id,
    body: {
      doc: data,
      doc_as_upsert: false
    }
  }, defaults)).then(({ body }) => {
    done(null, body);
  }).catch((error) => {
    log('ESConnector.prototype.save', error.message);
    done(error);
  });
}

module.exports.save = save;
