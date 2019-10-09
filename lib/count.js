const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function count(modelName, done, where) {
  const self = this;
  log('ESConnector.prototype.count', 'model', modelName, 'where', where);

  const idName = self.idName(modelName);
  const body = where.native ? where.native : {
    query: self.buildWhere(modelName, idName, where).query
  };

  const defaults = self.addDefaults(modelName, 'count');
  self.db.count(_.defaults({
    body
  }, defaults)).then(
    (response) => {
      done(null, response.count);
    },
    (err) => {
      log('ESConnector.prototype.count', err.message);
      return done(err, null);
    }
  );
}

module.exports.count = count;
