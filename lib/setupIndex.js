'use strict';

var log = null;
var Promise = null;

var createIndex = function createIndex(self, params) {
  log('ESConnector.prototype.setupIndices', 'createIndex()', params);
  return self.db.indices.create(params)
    .then(function (info) {
      log('ESConnector.prototype.setupIndices', 'createIndex()', 'self.db.indices.create()', 'response:', info);
      return Promise.resolve();
    }, function (err) {
      if (err.message.indexOf('IndexAlreadyExistsException') !== -1 ||
        err.message.indexOf('index_already_exists_exception') !== -1) {
        //console.trace('OMG WTF', err);
        log('ESConnector.prototype.setupIndices', 'createIndex()', 'self.db.indices.create()', 'we ate IndexAlreadyExistsException');
        return Promise.resolve();
      } else {
        log('ESConnector.prototype.setupIndices', 'createIndex()', 'self.db.indices.create()', 'failed:', err);
        return Promise.reject(err);
      }
    });
};

var setupIndex = function setupIndex(indexName) {
  var self = this;

  if (!indexName) { // validate input
    return Promise.reject('missing indexName');
  }

  var params = {
    index: indexName,
    body: self.searchIndexSettings
  };
  return self.db.indices.exists(params)
    .then(function (exists) {
      log('ESConnector.prototype.setupIndices', 'self.db.indices.exists()', 'response:', exists);
      if (!exists) {
        return createIndex(self, params);
      } else {
        return Promise.resolve();
      }
    }, function (err) {
      log('ESConnector.prototype.setupIndices', 'self.db.indices.exists()', 'failed:', err);
      return Promise.reject(err);
    });
};

module.exports = function (dependencies) {
  log = (dependencies) ? (dependencies.log || console.log) : console.log; /*eslint no-console: ['error', { allow: ['log'] }] */
  Promise = (dependencies) ? (dependencies.bluebird || require('bluebird')) : require('bluebird');
  return setupIndex;
};