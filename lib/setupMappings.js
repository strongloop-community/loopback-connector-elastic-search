'use strict';

// var _ = null;
var log = null;
var Promise = null;

var setupMappings = function () {
  var self = this;
  // var modelNames = _.map(self.settings.mappings, 'name');
  var mappingTypes = [(self.settings.mappingType || 'basedata')];

  return Promise.map(mappingTypes, function (mappingType) {
    log('ESConnector.prototype.setupMappings', 'will setup mapping for mappingType:', mappingType);
    return self.setupMapping(mappingType).then(function (body) {
      log('ESConnector.prototype.setupMappings', 'finished mapping for mappingType:', mappingType, 'response:', body);
      return Promise.resolve();
    }, function (err) {
      log('ESConnector.prototype.setupMappings', 'failed mapping for mappingType:', mappingType, 'err:', err);
      return Promise.reject(err);
    });
  }, {
    concurrency: 1
  }).then(function () {
    log('ESConnector.prototype.setupMappings', 'finished');
    return Promise.resolve();
  }).catch(function (err) {
    log('ESConnector.prototype.setupMappings', 'failed', err);
    return Promise.reject(err);
  });
};

module.exports = function (dependencies) {
  log = (dependencies) ? (dependencies.log || console.log) : console.log; /*eslint no-console: ['error', { allow: ['log'] }] */
  // _ = (dependencies) ? (dependencies.lodash ||  require('lodash')) : require('lodash');
  Promise = (dependencies) ? (dependencies.bluebird || require('bluebird')) : require('bluebird');
  return setupMappings;
};