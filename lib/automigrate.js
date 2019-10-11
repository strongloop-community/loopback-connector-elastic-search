let log = null;
const _ = require('lodash');

/**
 * `Connector._models` are all known at the time `automigrate` is called
 *  so it should be possible to work on all elasticsearch indicies and mappings at one time
 *  unlike with `.connect()` when the models were still unknown so
 *  initializing ES indicies and mappings in one go wasn't possible.
 *
 * @param models
 * @param cb
 */
const automigrate = (models, cb) => {
  log('ESConnector.prototype.automigrate', 'models:', models);
  const self = this;
  if (self.db) {
    if (!cb && (typeof models === 'function')) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if (typeof models === 'string') {
      models = [models];
    }
    log('ESConnector.prototype.automigrate', 'models', models);

    // eslint-disable-next-line no-underscore-dangle
    models = models || Object.keys(self._models);

    let indices = [];
    let mappingTypes = [];

    _.forEach(models, (model) => {
      log('ESConnector.prototype.automigrate', 'model', model);
      const defaults = self.addDefaults(model);
      mappingTypes.push(defaults.type);
      indices.push(defaults.index);
    });

    indices = _.uniq(indices);
    mappingTypes = _.uniq(mappingTypes);

    log('ESConnector.prototype.automigrate', 'calling self.db.indices.delete() for indices:', indices);
    cb();
    // TODO:
    /*
      self.db.indices.delete({index: indices, ignore: 404})
        .then(function(response) {
          log('ESConnector.prototype.automigrate', 'finished deleting all indices', response);
          return Promise.map(
              mappingTypes,
              function(mappingType){
                return self.setupMapping(mappingType);
              },
              {concurrency: 1}
          )
              .then(function(){
                log('ESConnector.prototype.automigrate', 'finished all mappings');
                cb();
              });
        })
        .catch(function(err){
          log('ESConnector.prototype.automigrate', 'failed', err);
          cb(err);
        });
    */
  } else {
    log('ESConnector.prototype.automigrate', 'ERROR', 'Elasticsearch connector has not been initialized');
    cb('Elasticsearch connector has not been initialized');
  }
};

module.exports = (dependencies) => {
  log = dependencies
    // eslint-disable-next-line no-console
    ? (dependencies.log || console.log)
    // eslint-disable-next-line no-console
    : console.log;
  return automigrate;
};
