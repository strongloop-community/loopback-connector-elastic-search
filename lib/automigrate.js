'use strict';

var log = null;
var _ = null;
var Promise = null;

/**
 * `Connector._models` are all known at the time `automigrate` is called
 *  so it should be possible to work on all elasticsearch indicies and mappings at one time
 *  unlike with `.connect()` when the models were still unknown so
 *  initializing ES indicies and mappings in one go wasn't possible.
 *
 * @param models
 * @param cb
 */
var automigrate = function (models, cb) {
  log('ESConnector.prototype.automigrate', 'models:', models);
  var self = this;
  if (self.db) {
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }
    log('ESConnector.prototype.automigrate', 'models', models);

    models = models || Object.keys(self._models);

    var indices = [];

    _.forEach(models, function (model){
      log('ESConnector.prototype.automigrate', 'model', model);
      var defaults = self.addDefaults(model);
      indices.push(defaults.index);
    });

    indices = _.uniq(indices);

    log('ESConnector.prototype.automigrate', 'calling self.db.indices.delete() for indices:', indices);
    //cb(); // TODO:
    self.db.indices.delete({index: indices, ignore: 404})
        .then(function(response) {
          log('ESConnector.prototype.automigrate', 'finished deleting all indices', response);
          return Promise.map(
              models,
              function(modelName){
                return self.setupMapping(modelName);
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
  }
  else {
    log('ESConnector.prototype.automigrate', 'ERROR', 'Elasticsearch connector has not been initialized');
    cb('Elasticsearch connector has not been initialized');
  }
};

module.exports = function(dependencies) {
  log = (dependencies) ? (dependencies.log || console.log) : console.log; /*eslint no-console: ["error", { allow: ["log"] }] */
  _ = (dependencies) ? (dependencies.lodash ||  require('lodash')) : require('lodash');
  Promise = (dependencies) ? (dependencies.bluebird ||  require('bluebird')) : require('bluebird');
  return automigrate;
};
