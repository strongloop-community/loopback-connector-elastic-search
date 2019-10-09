const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

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
  self.db.update(_.defaults({
    id,
    body: {
      doc: data,
      doc_as_upsert: false
    }
  }, defaults)).then((response) => {
    done(null, response);
  }, (err) => done(err, null));
}

module.exports.save = save;
