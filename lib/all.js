const log = require('debug')('loopback:connector:elasticsearch');

function all(model, filter, done) {
  const self = this;
  log('ESConnector.prototype.all', 'model', model, 'filter', JSON.stringify(filter, null, 0));

  const idName = self.idName(model);
  log('ESConnector.prototype.all', 'idName', idName);

  if (filter && filter.suggests) { // TODO: remove HACK!!!
    self.db.suggest(
      self.buildFilter(model, idName, filter, self.defaultSize)
    ).then(
      (body) => {
        const result = [];
        if (body.hits) {
          body.hits.hits.forEach((item) => {
            result.push(self.dataSourceToModel(model, item, idName));
          });
        }
        log('ESConnector.prototype.all', 'model', model, 'result', JSON.stringify(result, null, 2));
        if (filter && filter.include) {
          // eslint-disable-next-line no-underscore-dangle
          self._models[model].model.include(result, filter.include, done);
        } else {
          done(null, result);
        }
      },
      (err) => {
        log('ESConnector.prototype.all', err.message);
        return done(err, null);
      }
    );
  } else {
    self.db.search(
      self.buildFilter(model, idName, filter, self.defaultSize)
    ).then(
      (body) => {
        const result = [];
        body.hits.hits.forEach((item) => {
          result.push(self.dataSourceToModel(model, item, idName));
        });
        log('ESConnector.prototype.all', 'model', model, 'result', JSON.stringify(result, null, 2));
        if (filter && filter.include) {
          // eslint-disable-next-line no-underscore-dangle
          self._models[model].model.include(result, filter.include, done);
        } else {
          done(null, result);
        }
      },
      (err) => {
        log('ESConnector.prototype.all', err.message);
        return done(err, null);
      }
    );
  }
}

module.exports.all = all;
