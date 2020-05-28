const log = require('debug')('loopback:connector:elasticsearch');

function all(model, filter, done) {
  const self = this;
  log('ESConnector.prototype.all', 'model', model, 'filter', JSON.stringify(filter, null, 0));

  const idName = self.idName(model);
  log('ESConnector.prototype.all', 'idName', idName);

  self.db.search(
    self.buildFilter(model, idName, filter, self.defaultSize)
  ).then(
    ({ body }) => {
      const result = [];
      const totalCount = typeof body.hits.total === 'object' ? body.hits.total.value : body.hits.total;
      body.hits.hits.forEach((item) => {
        result.push(self.dataSourceToModel(model, item, idName, totalCount));
      });
      log('ESConnector.prototype.all', 'model', model, 'result', JSON.stringify(result, null, 2));
      if (filter && filter.include) {
        // eslint-disable-next-line no-underscore-dangle
        self._models[model].model.include(result, filter.include, done);
      } else {
        done(null, result);
      }
    }
  ).catch((error) => {
    log('ESConnector.prototype.all', error.message);
    return done(error, null);
  });
}

module.exports.all = all;
