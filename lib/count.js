const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function count(modelName, done, where) {
  const self = this;
  log('ESConnector.prototype.count', 'model', modelName, 'where', where);

  const idName = self.idName(modelName);
  const query = {
    query: self.buildWhere(modelName, idName, where).query
  };

  const defaults = self.addDefaults(modelName, 'count');
  self.db.count(_.defaults({
    body: query
  }, defaults)).then(({ body }) => {
    done(null, body.count);
  }).catch((error) => {
    log('ESConnector.prototype.count', error.message);
    done(error, null);
  });
}

module.exports.count = count;
