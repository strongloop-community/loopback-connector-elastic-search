'use strict';

var _ = null;
var log = null;
var Promise = null;

var setupMapping = function (mappingType) {
  if (!mappingType) {
    return Promise.reject('missing mappingType');
  }
  var self = this;
  var db = self.db;
  var settings = self.settings;

  var mappingsFromDatasource = [{
    'name': mappingType,
    'properties': settings.mappingProperties
  }]
  log('ESConnector.prototype.setupMapping', 'mappingsFromDatasource:', mappingsFromDatasource);

  var mappingFromDatasource;
  if (mappingsFromDatasource.length === 0) {
    log('ESConnector.prototype.setupMapping', 'missing mapping for mappingType:', mappingType,
      ' ... this usecase is legitimate if you want elasticsearch to take care of mapping dynamically');
    return Promise.resolve();
  } else if (mappingsFromDatasource.length > 1) {
    return Promise.reject('more than one mapping for mappingType:', mappingType);
  } else {
    log('ESConnector.prototype.setupMapping', 'found mapping for mappingType:', mappingType);
    mappingFromDatasource = mappingsFromDatasource[0];
  }

  var defaults = self.addDefaults(mappingFromDatasource.name); // NOTE: this is where the magic happens
  var mapping = _.clone(mappingFromDatasource);

  // TODO: create a method called cleanUpMapping or something like that to blackbox this stuff
  delete mapping.name;
  // delete mapping.index;
  delete mapping.type;

  // adding 'docType' mandatory keyword field to mapping properties
  mapping.properties.docType = {
    'type': 'keyword',
    'index': true
  };

  log('ESConnector.prototype.setupMapping', 'will setup mapping for mappingType:', mappingFromDatasource.name);

  //return self.setupIndices(defaults.index)
  return self.setupIndex(defaults.index).then(function () {
    log('ESConnector.prototype.setupMapping', 'db.indices.putMapping', 'mappingType:', mappingType, 'start');
    return db.indices.putMapping(_.defaults({
      body: mapping
    }, defaults)).then(function (body) {
      log('ESConnector.prototype.setupMapping', 'db.indices.putMapping', 'mappingType:', mappingType, 'response', body);
      return Promise.resolve();
    }, function (err) {
      log('ESConnector.prototype.setupMapping', 'db.indices.putMapping', 'mappingType:', mappingType, 'failed', err);
      //console.trace(err.message);
      return Promise.reject(err);
    });
  });
};

module.exports = function (dependencies) {
  log = (dependencies) ? (dependencies.log || console.log) : console.log; /*eslint no-console: ['error', { allow: ['log'] }] */
  _ = (dependencies) ? (dependencies.lodash || require('lodash')) : require('lodash');
  Promise = (dependencies) ? (dependencies.bluebird || require('bluebird')) : require('bluebird');
  return setupMapping;
};