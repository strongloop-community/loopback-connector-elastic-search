/* eslint-disable func-names */
const util = require('util');
const fs = require('fs');
const path = require('path');
const _ = require('lodash');

const log = require('debug')('loopback:connector:elasticsearch');

const elasticsearch = require('elasticsearch');
const { Connector } = require('loopback-connector');
const automigrate = require('./automigrate.js')({
  log
});
const setupMapping = require('./setupMapping.js')({
  log
});
const setupIndex = require('./setupIndex.js')({
  log
});
const { all } = require('./all');
const { buildDeepNestedQueries } = require('./buildDeepNestedQueries');
const { buildNestedQueries } = require('./buildNestedQueries');
const { buildFilter } = require('./buildFilter');
const { buildOrder } = require('./buildOrder');
const { buildWhere } = require('./buildWhere');
const { count } = require('./count');
const { create } = require('./create');
const { destroy } = require('./destroy');
const { destroyAll } = require('./destroyAll');
const { exists } = require('./exists');
const { find } = require('./find');
const { replaceById } = require('./replaceById');
const { replaceOrCreate } = require('./replaceOrCreate');
const { save } = require('./save');
const { updateAll } = require('./updateAll');
const { updateAttributes } = require('./updateAttributes');
const { updateOrCreate } = require('./updateOrCreate');

/**
 * Connector constructor
 * @param {object} datasource settings
 * @param {object} dataSource
 * @constructor
 */
class ESConnector {
  constructor(settings, dataSource) {
    Connector.call(this, 'elasticsearch', settings);
    const defaultRefreshIndexAPIs = [
      'create',
      'save',
      'destroy',
      'destroyAll',
      'updateAttributes',
      'updateOrCreate',
      'updateAll',
      'replaceOrCreate',
      'replaceById'
    ];
    this.searchIndex = settings.index || 'shakespeare';
    this.searchIndexSettings = settings.settings || {};
    this.searchType = settings.mappingType || 'basedata';
    this.defaultSize = (settings.defaultSize || 50);
    this.idField = 'id';
    this.apiVersion = (settings.apiVersion || '6.0');
    this.refreshOn = (settings.refreshOn || defaultRefreshIndexAPIs);

    this.debug = settings.debug || log.enabled;
    if (this.debug) {
      log('Settings: %j', settings);
    }

    this.dataSource = dataSource;
  }
}

/**
 * Initialize connector with datasource, configure settings and return
 * @param {object} dataSource
 * @param {function} done callback
 */
module.exports.initialize = (dataSource, callback) => {
  if (!elasticsearch) {
    return;
  }

  const settings = dataSource.settings || {};

  dataSource.connector = new ESConnector(settings, dataSource);

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

/**
 * Inherit the prototype methods
 */
util.inherits(ESConnector, Connector);

/**
 * Generate a client configuration object based on settings.
 */
ESConnector.prototype.getClientConfig = function () {
  // http://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
  const config = {
    hosts: this.settings.hosts || {
      host: '127.0.0.1',
      port: 9200
    },
    requestTimeout: this.settings.requestTimeout,
    apiVersion: this.settings.apiVersion,
    log: this.settings.log || 'error',
    suggestCompression: true
  };

  if (this.settings.amazonES) {
    // Remove AWS ES support for now
  }

  if (this.settings.ssl) {
    config.ssl = {
      ca: (this.settings.ssl.ca) ? fs.readFileSync(path.join(__dirname, this.settings.ssl.ca)) : fs.readFileSync(path.join(__dirname, '..', 'cacert.pem')),
      rejectUnauthorized: this.settings.ssl.rejectUnauthorized || true
    };
  }
  // Note: http://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
  // Due to the complex nature of the configuration, the config object you pass in will be modified
  // and can only be used to create one Client instance.
  // Related Github issue: https://github.com/elasticsearch/elasticsearch-js/issues/33
  // Luckily getClientConfig() pretty much clones settings so we shouldn't have to worry about it.
  return config;
};

/**
 * Connect to Elasticsearch client
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Db} db The elasticsearch client
 */
ESConnector.prototype.connect = function (callback) {
  // TODO: throw error if callback isn't provided?
  //       what are the corner-cases when the loopback framework does not provide callback
  //       and we need to be able to live with that?
  const self = this;
  if (self.db) {
    process.nextTick(() => {
      callback(null, self.db);
    });
  } else {
    self.db = new elasticsearch.Client(self.getClientConfig());
    self.db.ping({
      requestTimeout: self.settings.requestTimeout
    }, (error) => {
      if (error) {
        // eslint-disable-next-line no-console
        console.log('ESConnector.prototype.connect', 'ping', 'failed', error);
        log('ESConnector.prototype.connect', 'ping', 'failed', error);
      }
    });
    if (self.settings.mappingProperties) {
      self.setupMapping()
        .then(() => {
          log('ESConnector.prototype.connect', 'setupMappings', 'finished');
          callback(null, self.db);
        })
        .catch((err) => {
          log('ESConnector.prototype.connect', 'setupMappings', 'failed', err);
          callback(null, self.db);
        });
    } else {
      process.nextTick(() => {
        callback(null, self.db);
      });
    }
  }
};

/**
 * Delete a mapping (type definition) along with its data.
 *
 * @param modelNames
 * @param callback
 */
ESConnector.prototype.removeMappings = function (modelNames, callback) {
  const self = this;
  const {
    db,
    settings
  } = self;
  if (_.isFunction(modelNames)) {
    callback = modelNames;
    modelNames = _.map(settings.mappings, 'name');
  }
  log('ESConnector.prototype.removeMappings', 'modelNames', modelNames);

  const mappingsToRemove = _.filter(settings.mappings,
    (mapping) => !modelNames || _.includes(modelNames, mapping.name));

  log('ESConnector.prototype.removeMappings', 'mappingsToRemove', _.map(mappingsToRemove, 'name'));

  Promise.map(mappingsToRemove, (mapping) => {
    const defaults = self.addDefaults(mapping.name, 'removeMappings');
    log('ESConnector.prototype.removeMappings', 'calling self.db.indices.existsType()');
    return db.indices.existsType(defaults).then((result) => {
      if (!result) return Promise.resolve();
      log('ESConnector.prototype.removeMappings', 'calling self.db.indices.deleteMapping()');
      return db.indices.deleteMapping(defaults).then((body) => {
        log('ESConnector.prototype.removeMappings', mapping.name, body);
        return Promise.resolve();
      }, (err) => {
        log('ESConnector.prototype.removeMappings', err.message);
        return Promise.reject(err);
      });
    }, (err) => {
      log('ESConnector.prototype.removeMappings', err.message);
      return Promise.reject(err);
    });
  }, {
    concurrency: 1
  }).then(() => {
    log('ESConnector.prototype.removeMappings', 'finished');
    callback(null, self.db); // TODO: what does the connector framework want back as arguments here?
  }).catch((err) => {
    log('ESConnector.prototype.removeMappings', 'failed');
    callback(err);
  });
};

ESConnector.prototype.setupMapping = setupMapping;

ESConnector.prototype.setupIndex = setupIndex;


/**
 * Ping to test elastic connection
 * @returns {String} with ping result
 */
ESConnector.prototype.ping = function (cb) {
  const self = this;
  self.db.ping({
    requestTimeout: self.settings.requestTimeout
  }, (error) => {
    if (error) {
      log('Could not ping ES.');
      cb(error);
    } else {
      log('Pinged ES successfully.');
      cb();
    }
  });
};

/**
 * Return connector type
 * @returns {String} type description
 */
ESConnector.prototype.getTypes = function () {
  return [this.name];
};

/**
 * Get value from property checking type
 * @param {object} property
 * @param {String} value
 * @returns {object}
 */
ESConnector.prototype.getValueFromProperty = function (property, value) {
  if (property.type instanceof Array) {
    if (!value || (value.length === 0)) {
      return [];
    }
    return value;
  } if (property.type === String) {
    return value.toString();
  } if (property.type === Number) {
    return Number(value);
  } if (property.type === Date) {
    return new Date(value);
  }
  return value;
};

/**
 * Match and transform data structure to modelName
 * @param {String} modelName name
 * @param {Object} data from DB
 * @returns {object} modeled document
 */
ESConnector.prototype.matchDataToModel = function (modelName, data, esId, idName) {
  /*
  log('ESConnector.prototype.matchDataToModel', 'modelName',
    modelName, 'data', JSON.stringify(data,null,0));
  */
  const self = this;
  if (!data) {
    return null;
  }
  try {
    const document = {};

    // eslint-disable-next-line no-underscore-dangle
    const { properties } = this._models[modelName];
    _.assign(document, data); // it can't be this easy, can it?
    document[idName] = esId;

    Object.keys(properties).forEach((propertyName) => {
      const propertyValue = data[propertyName];
      // log('ESConnector.prototype.matchDataToModel', propertyName, propertyValue);
      if (propertyValue !== undefined && propertyValue !== null) {
        document[propertyName] = self.getValueFromProperty(
          properties[propertyName],
          propertyValue
        );
      }
    });
    log('ESConnector.prototype.matchDataToModel', 'document', JSON.stringify(document, null, 0));
    return document;
  } catch (err) {
    log('ESConnector.prototype.matchDataToModel', err.message);
    return null;
  }
};

/**
 * Convert data source to model
 * @param {String} model name
 * @param {Object} data object
 * @returns {object} modeled document
 */
ESConnector.prototype.dataSourceToModel = function (modelName, data, idName) {
  log('ESConnector.prototype.dataSourceToModel', 'modelName', modelName, 'data', JSON.stringify(data, null, 0));

  // return data._source; // TODO: super-simplify?
  // eslint-disable-next-line no-underscore-dangle
  return this.matchDataToModel(modelName, data._source, data._id, idName);
};

/**
 * Add defaults such as index name and type
 *
 * @param {String} modelName
 * @param {String} functionName The caller function name
 * @returns {object} Filter with index and type
 */
ESConnector.prototype.addDefaults = function (modelName, functionName) {
  const self = this;
  log('ESConnector.prototype.addDefaults', 'modelName:', modelName);

  // TODO: fetch index and type from `self.settings.mappings` too
  const indexFromDatasource = self.settings.index;
  const typeFromDatasource = self.settings.mappingType;
  const filter = {};
  if (this.searchIndex) {
    filter.index = indexFromDatasource || this.searchIndex;
  }
  filter.type = typeFromDatasource || this.searchType || modelName;

  // When changing data, wait until the change has been indexed...
  // ...so it is instantly available for search
  if (this.refreshOn.indexOf(functionName) !== -1) {
    filter.refresh = true;
  }

  // eslint-disable-next-line no-underscore-dangle
  const modelClass = this._models[modelName];
  if (modelClass && _.isObject(modelClass.settings.elasticsearch)
    && _.isObject(modelClass.settings.elasticsearch.settings)) {
    _.extend(filter, modelClass.settings.elasticsearch.settings);
  }

  if (functionName && modelClass && _.isObject(modelClass.settings.elasticsearch)
    && _.isObject(modelClass.settings.elasticsearch[functionName])) {
    _.extend(filter, modelClass.settings.elasticsearch[functionName]);
  }

  log('ESConnector.prototype.addDefaults', 'filter:', filter);
  return filter;
};

/**
 * Make filter from criteria, data index and type
 * Ex:
 *   {'body': {'query': {'match': {'title': 'Futuro'}}}}
 *   {'q' : 'Futuro'}
 * @param {String} modelName filter
 * @param {Object} criteria filter
 * @param {number} size of rows to return, if null then skip
 * @param {number} offset to return, if null then skip
 * @returns {object} filter
 */
ESConnector.prototype.buildFilter = buildFilter;

/**
 * 1. Words of wisdom from @doublemarked:
 *    > When writing a query without an order specified,
      the author should not assume any reliable order.
 *    > So if we’re not assuming any order,
      there is not a compelling reason to potentially slow down
 *    > the query by enforcing a default order.
 * 2. Yet, most connector implementations do enforce a default order ... what to do?
 *
 * @param model
 * @param idName
 * @param order
 * @returns {Array}
 */
ESConnector.prototype.buildOrder = buildOrder;

ESConnector.prototype.buildWhere = buildWhere;

ESConnector.prototype.buildNestedQueries = buildNestedQueries;

ESConnector.prototype.buildDeepNestedQueries = buildDeepNestedQueries;

/**
 * Get document Id validating data
 * @param {String} id
 * @returns {Number} Id
 * @constructor
 */
ESConnector.prototype.getDocumentId = function (id) {
  try {
    if (typeof id !== 'string') {
      return id.toString();
    }
    return id;
  } catch (e) {
    return id;
  }
};

/**
 * Implement CRUD Level I - Key methods to be implemented by a connector to support full CRUD
 * > Create a new model instance
 *   > CRUDConnector.prototype.create = function (model, data, callback) {...};
 * > Query model instances by filter
 *   > CRUDConnector.prototype.all = function (model, filter, callback) {...};
 * > Delete model instances by query
 *   > CRUDConnector.prototype.destroyAll = function (model, where, callback) {...};
 * > Update model instances by query
 *   > CRUDConnector.prototype.updateAll = function (model, where, data, callback) {...};
 * > Count model instances by query
 *   > CRUDConnector.prototype.count = function (model, callback, where) {...};
 * > getDefaultIdType
 *   > very useful for setting a default type for IDs like 'string' rather than 'number'
 };
 */

ESConnector.prototype.getDefaultIdType = function () {
  return String;
};
/**
 * Create a new model instance
 * @param {String} model name
 * @param {object} data info
 * @param {Function} done - invoke the callback with the created model's id as an argument
 */
ESConnector.prototype.create = create;

/**
 * Query model instances by filter
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} done callback function
 *
 * NOTE: UNLIKE create() where the ID is returned not as a part of the created content
 * but rather individually as an argument to the callback ... in the all() method
 * it makes sense to return the id with the content! So for a datasource like elasticsearch,
 * make sure to map '_id' into the content, just in case its an auto-generated one.
 */
ESConnector.prototype.all = all;

/**
 * Delete model instances by query
 * @param {String} modelName name
 * @param {String} whereClause criteria
 * @param {Function} cb callback
 */
ESConnector.prototype.destroyAll = destroyAll;

/**
 * Update model instances by query
 *
 * NOTES:
 * > Without an update by query plugin, this isn't supported by ES out-of-the-box
 * > To run updateAll these parameters should be enabled in elasticsearch config
 *   -> script.inline: true
 *   -> script.indexed: true
 *   -> script.engine.groovy.inline.search: on
 *   -> script.engine.groovy.inline.update: on
 *
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb - should be invoked with a second callback argument
 *                           that provides the count of affected rows in the callback
 *                           such as cb(err, {count: affectedRows}).
 *                           Notice the second argument is an object with the count property
 *                           representing the number of rows that were updated.
 */
ESConnector.prototype.updateAll = updateAll;

ESConnector.prototype.update = ESConnector.prototype.updateAll;

/**
 * Count model instances by query
 * @param {String} model name
 * @param {String} where criteria
 * @param {Function} done callback
 */
ESConnector.prototype.count = count;

/**
 * Implement CRUD Level II - A connector can choose to implement the following methods,
 *                           otherwise, they will be mapped to those from CRUD Level I.
 * > Find a model instance by id
 *   > CRUDConnector.prototype.find = function (model, id, callback) {...};
 * > Delete a model instance by id
 *   > CRUDConnector.prototype.destroy = function (model, id, callback) {...};
 * > Update a model instance by id
 *   > CRUDConnector.prototype.updateAttributes = function (model, id, data, callback) {...};
 * > Check existence of a model instance by id
 *   > CRUDConnector.prototype.exists = function (model, id, callback) {...};
 */

/**
 * Find a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {Function} done callback
 */
ESConnector.prototype.find = find;

/**
 * Delete a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {Function} done callback
 */
ESConnector.prototype.destroy = destroy;

/**
 * Update a model instance by id
 *
 */

ESConnector.prototype.updateAttributes = updateAttributes;

/**
 * Check existence of a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {function} done callback
 */
ESConnector.prototype.exists = exists;

/**
 * Implement CRUD Level III - A connector can also optimize certain methods
 *                            if the underlying database provides native/atomic
 *                            operations to avoid multiple calls.
 * > Save a model instance
 *   > CRUDConnector.prototype.save = function (model, data, callback) {...};
 * > Find or create a model instance
 *   > CRUDConnector.prototype.findOrCreate = function (model, data, callback) {...};
 * > Update or insert a model instance
 *   > CRUDConnector.prototype.updateOrCreate = function (model, data, callback) {...};
 */

/**
 * Update document data
 * @param {String} model name
 * @param {Object} data document
 * @param {Function} done callback
 */
ESConnector.prototype.save = save;

/**
 * Find or create a model instance
 */
// ESConnector.prototype.findOrCreate = function (model, data, callback) {...};

/**
 * Update or insert a model instance
 * @param modelName
 * @param data
 * @param callback - should pass the following arguments to the callback:
 *                   err object (null on success)
 *                   data object containing the property values as found in the database
 *                   info object providing more details about the result of the operation.
 *                               At the moment, it should have a single property isNewInstance
 *                               with the value true if a new model was created
 *                               and the value false is an existing model was found & updated.
 */
ESConnector.prototype.updateOrCreate = updateOrCreate;

/**
 * replace or insert a model instance
 * @param modelName
 * @param data
 * @param callback - should pass the following arguments to the callback:
 *                   err object (null on success)
 *                   data object containing the property values as found in the database
 *                   info object providing more details about the result of the operation.
 *                               At the moment, it should have a single property isNewInstance
 *                               with the value true if a new model was created
 *                               and the value false is an existing model was found & updated.
 */
ESConnector.prototype.replaceOrCreate = replaceOrCreate;

ESConnector.prototype.replaceById = replaceById;

/**
 * Migration
 *   automigrate - Create/recreate DB objects (such as table/column/constraint/trigger/index)
 *                 to match the model definitions
 *   autoupdate - Alter DB objects to match the model definitions
 */

/**
 * Perform automigrate for the given models. Create/recreate DB objects
 * (such as table/column/constraint/trigger/index) to match the model definitions
 *  --> Drop the corresponding indices: both mappings and data are done away with
 *  --> create/recreate mappings and indices
 *
 * @param {String[]} [models] A model name or an array of model names.
 * If not present, apply to all models
 * @param {Function} [cb] The callback function
 */
ESConnector.prototype.automigrate = automigrate;

module.exports.name = ESConnector.name;
module.exports.ESConnector = ESConnector;
