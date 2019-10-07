const util = require('util');
const fs = require('fs');
const path = require('path');
const _ = require('lodash');

const log = require('debug')('loopback:connector:elasticsearch');

const elasticsearch = require('elasticsearch');
const {
  Connector
} = require('loopback-connector');

/**
 * Connector constructor
 * @param {object} datasource settings
 * @param {object} dataSource
 * @constructor
 */
const ESConnector = (settings, dataSource) => {
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
};

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
ESConnector.prototype.getClientConfig = () => {
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
ESConnector.prototype.connect = (callback) => {
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
ESConnector.prototype.removeMappings = (modelNames, callback) => {
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
    return db.indices.existsType(defaults).then((exists) => {
      if (!exists) return Promise.resolve();
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

ESConnector.prototype.setupMapping = require('./setupMapping.js')({
  log,
  lodash: _
});

ESConnector.prototype.setupIndex = require('./setupIndex.js')({
  log
});


/**
 * Ping to test elastic connection
 * @returns {String} with ping result
 */
ESConnector.prototype.ping = (cb) => {
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
ESConnector.prototype.getTypes = () => [this.name];

/**
 * Get value from property checking type
 * @param {object} property
 * @param {String} value
 * @returns {object}
 */
ESConnector.prototype.getValueFromProperty = (property, value) => {
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
ESConnector.prototype.matchDataToModel = (modelName, data, esId, idName) => {
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
ESConnector.prototype.dataSourceToModel = (modelName, data, idName) => {
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
ESConnector.prototype.addDefaults = (modelName, functionName) => {
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
ESConnector.prototype.buildFilter = (modelName, idName, criteria, size, offset) => {
  const self = this;
  log('ESConnector.prototype.buildFilter', 'model', modelName, 'idName', idName,
    'criteria', JSON.stringify(criteria, null, 0));

  if (idName === undefined || idName === null) {
    throw new Error('idName not set!');
  }

  const filter = this.addDefaults(modelName, 'buildFilter');
  filter.body = {};

  if (size !== undefined && size !== null) {
    filter.size = size;
  }
  if (offset !== undefined && offset !== null) {
    filter.from = offset;
  }

  if (criteria) {
    // `criteria` is set by app-devs, therefore, it overrides any connector level arguments
    if (criteria.limit !== undefined && criteria.limit !== null) {
      filter.size = criteria.limit;
    }
    if (criteria.skip !== undefined && criteria.skip !== null) {
      filter.from = criteria.skip;
    } else if (criteria.offset !== undefined
        && criteria.offset !== null) { // use offset as an alias for skip
      filter.from = criteria.offset;
    }
    if (criteria.fields) {
      // { fields: {propertyName: <true|false>, propertyName: <true|false>, ... } }
      // filter.body.fields = self.buildOrder(model, idName, criteria.fields);
      // TODO: make it so
      // http://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-request-source-filtering.html
      // http://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-request-fields.html
      /* POST /shakespeare/User/_search
       {
       '_source': {
       'include': ['seq'],
       'exclude': ['seq']
       }
       } */

      /* @raymondfeng and @bajtos - I'm observing something super strange,
       i haven't implemented the FIELDS filter for elasticsearch connector
       but the test which should fail until I implement such a feature ... is actually passing!
       ... did someone at some point of time implement an in-memory filter for FIELDS
       in the underlying loopback-connector implementation? */

      // Elasticsearch _source filtering code
      /* if (Array.isArray(criteria.fields) || typeof criteria.fields === 'string') {
        filter.body._source = criteria.fields;
      } else if (typeof criteria.fields === 'object' && Object.keys(criteria.fields).length > 0) {
        filter.body._source.includes = _.map(_.pickBy(criteria.fields, function(v, k) {
          return v === true;
        }), function(v, k) { return k; });
        filter.body._source.excludes = _.map(_.pickBy(criteria.fields, function(v, k) {
          return v === false;
        }), function(v, k) { return k; });
      } */
    }
    if (criteria.order) {
      log('ESConnector.prototype.buildFilter', 'will delegate sorting to buildOrder()');
      filter.body.sort = self.buildOrder(modelName, idName, criteria.order);
    } else { // TODO: expensive~ish and no clear guidelines so turn it off?
      // var idNames = this.idNames(model); // TODO: support for compound ids?
      // eslint-disable-next-line no-underscore-dangle
      const modelProperties = this._models[modelName].properties;
      if (idName === 'id' && modelProperties.id.generated) {
        // filter.body.sort = ['_id']; // requires mapping to contain: ...
        // ...'_id' : {'index' : 'not_analyzed','store' : true}
        log('ESConnector.prototype.buildFilter', 'will sort on _id by default when IDs are meant to be auto-generated by elasticsearch');
        filter.body.sort = ['_id'];
      } else {
        log('ESConnector.prototype.buildFilter', 'will sort on loopback specified IDs');
        filter.body.sort = [idName]; // default sort should be based on fields marked as id
      }
    }
    if (criteria.where) {
      filter.body.query = self.buildWhere(modelName, idName, criteria.where).query;
    } else if (criteria.suggests) { // TODO: remove HACK!!!
      filter.body = {
        suggest: criteria.suggests
      }; // assume that the developer has provided ES compatible DSL
    } else if (criteria.native) {
      filter.body = criteria.native; // assume that the developer has provided ES compatible DSL
    } else if (_.keys(criteria).length === 0) {
      filter.body = {
        query: {
          bool: {
            must: {
              match_all: {}
            },
            filter: [{
              term: {
                docType: modelName
              }
            }]
          }
        }
      };
    } else if (!Object.prototype.hasOwnProperty.call(criteria, 'where')) {
      // For queries without 'where' filter, add docType filter
      filter.body.query = self.buildWhere(modelName, idName, criteria.where || {}).query;
    }
  }

  log('ESConnector.prototype.buildFilter', 'constructed', JSON.stringify(filter, null, 0));
  return filter;
};

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
ESConnector.prototype.buildOrder = (model, idName, order) => {
  const sort = [];

  let keys = order;
  if (typeof keys === 'string') {
    keys = keys.split(',');
  }
  for (let index = 0, len = keys.length; index < len; index += 1) {
    const m = keys[index].match(/\s+(A|DE)SC$/);
    let key = keys[index];
    key = key.replace(/\s+(A|DE)SC$/, '').trim();
    if (key === 'id' || key === idName) {
      key = '_id';
    }
    if (m && m[1] === 'DE') {
      // sort[key] = -1;
      const temp = {};
      temp[key] = 'desc';
      sort.push(temp);
    } else {
      // sort[key] = 1;
      sort.push(key);
    }
  }

  return sort;
};

ESConnector.prototype.buildWhere = (model, idName, where) => {
  const self = this;

  let nestedFields = _.map(self.settings.mappingProperties, (val, key) => (val.type === 'nested' ? key : null));
  nestedFields = _.filter(nestedFields, (v) => v);
  log('ESConnector.prototype.buildWhere', 'model', model, 'idName', idName, 'where', JSON.stringify(where, null, 0));

  const body = {
    query: {
      bool: {
        must: [],
        should: [],
        filter: [],
        must_not: []
      }
    }
  };

  self.buildNestedQueries(body, model, idName, where, nestedFields);
  if (body && body.query && body.query.bool
    && body.query.bool.must && body.query.bool.must.length === 0) {
    delete body.query.bool.must;
  }
  if (body && body.query && body.query.bool
    && body.query.bool.filter && body.query.bool.filter.length === 0) {
    delete body.query.bool.filter;
  }
  if (body && body.query && body.query.bool
    && body.query.bool.should && body.query.bool.should.length === 0) {
    delete body.query.bool.should;
  }
  if (body && body.query && body.query.bool
    && body.query.bool.must_not && body.query.bool.must_not.length === 0) {
    delete body.query.bool.must_not;
  }
  if (body && body.query && body.query.bool && _.isEmpty(body.query.bool)) {
    delete body.query.bool;
  }

  if (body && body.query && _.isEmpty(body.query)) {
    body.query = {
      bool: {
        must: {
          match_all: {}
        },
        filter: [{
          term: {
            docType: model
          }
        }]
      }
    };
  }
  return body;
};

ESConnector.prototype.buildNestedQueries = (body, model, idName, where, nestedFields) => {
  /**
   * Return an empty match all object if no property is set in where filter
   * @example {where: {}}
   */
  if (_.keys(where).length === 0) {
    body = {
      query: {
        bool: {
          must: {
            match_all: {}
          },
          filter: [{
            term: {
              docType: model
            }
          }]
        }
      }
    };
    log('ESConnector.prototype.buildNestedQueries', '\nbody', JSON.stringify(body, null, 0));
    return body;
  }
  const rootPath = body.query;
  ESConnector.prototype.buildDeepNestedQueries(true, idName, where,
    body, rootPath, model, nestedFields);
  const docTypeQuery = _.find(rootPath.bool.filter, (v) => v.term && v.term.docType);
  let addedDocTypeToRootPath = false;
  if (typeof docTypeQuery !== 'undefined') {
    addedDocTypeToRootPath = true;
    docTypeQuery.term.docType = model;
  } else {
    addedDocTypeToRootPath = true;
    rootPath.bool.filter.push({
      term: {
        docType: model
      }
    });
  }

  if (addedDocTypeToRootPath) {
    if (!!rootPath && rootPath.bool && rootPath.bool.should && rootPath.bool.should.length !== 0) {
      rootPath.bool.must.push({
        bool: {
          should: rootPath.bool.should
        }
      });
      rootPath.bool.should = [];
    }

    if (!!rootPath && rootPath.bool
      && rootPath.bool.must_not && rootPath.bool.must_not.length !== 0) {
      rootPath.bool.must.push({
        bool: {
          must_not: rootPath.bool.must_not
        }
      });
      rootPath.bool.must_not = [];
    }
  }
  return true;
};

ESConnector.prototype.buildDeepNestedQueries = (
  root,
  idName,
  where,
  body,
  queryPath,
  model,
  nestedFields
) => {
  const self = this;
  _.forEach(where, (value, key) => {
    let cond = value;
    if (key === 'id' || key === idName) {
      key = '_id';
    }
    const splitKey = key.split('.');
    let isNestedKey = false;
    let nestedSuperKey = null;
    if (key.indexOf('.') > -1 && !!splitKey[0] && nestedFields.indexOf(splitKey[0]) > -1) {
      isNestedKey = true;
      // eslint-disable-next-line prefer-destructuring
      nestedSuperKey = splitKey[0];
    }

    if (key === 'and' && Array.isArray(value)) {
      let andPath;
      if (root) {
        andPath = queryPath.bool.must;
      } else {
        const andObject = {
          bool: {
            must: []
          }
        };
        andPath = andObject.bool.must;
        queryPath.push(andObject);
      }
      cond.forEach((c) => {
        log('ESConnector.prototype.buildDeepNestedQueries', 'mapped', 'body', JSON.stringify(body, null, 0));
        self.buildDeepNestedQueries(false, idName, c, body, andPath, model, nestedFields);
      });
    } else if (key === 'or' && Array.isArray(value)) {
      let orPath;
      if (root) {
        orPath = queryPath.bool.should;
      } else {
        const orObject = {
          bool: {
            should: []
          }
        };
        orPath = orObject.bool.should;
        queryPath.push(orObject);
      }
      cond.forEach((c) => {
        log('ESConnector.prototype.buildDeepNestedQueries', 'mapped', 'body', JSON.stringify(body, null, 0));
        self.buildDeepNestedQueries(false, idName, c, body, orPath, model, nestedFields);
      });
    } else {
      let spec = false;
      let options = null;
      if (cond && cond.constructor.name === 'Object') { // need to understand
        options = cond.options;
        // eslint-disable-next-line prefer-destructuring
        spec = Object.keys(cond)[0];
        cond = cond[spec];
      }
      log('ESConnector.prototype.buildNestedQueries',
        'spec', spec, 'key', key, 'cond', JSON.stringify(cond, null, 0), 'options', options);
      if (spec) {
        if (spec === 'gte' || spec === 'gt' || spec === 'lte' || spec === 'lt') {
          let rangeQuery = {
            range: {}
          };
          const rangeQueryGuts = {};
          rangeQueryGuts[spec] = cond;
          rangeQuery.range[key] = rangeQueryGuts;

          // Additional handling for nested Objects
          if (isNestedKey) {
            rangeQuery = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: rangeQuery
              }
            };
          }

          if (root) {
            queryPath.bool.must.push(rangeQuery);
          } else {
            queryPath.push(rangeQuery);
          }
        }

        /**
         * Logic for loopback `between` filter of where
         * @example {where: {size: {between: [0,7]}}}
         */
        if (spec === 'between') {
          if (cond.length === 2 && (cond[0] <= cond[1])) {
            let betweenArray = {
              range: {}
            };
            betweenArray.range[key] = {
              gte: cond[0],
              lte: cond[1]
            };

            // Additional handling for nested Objects
            if (isNestedKey) {
              betweenArray = {
                nested: {
                  path: nestedSuperKey,
                  score_mode: 'max',
                  query: betweenArray
                }
              };
            }
            if (root) {
              queryPath.bool.must.push(betweenArray);
            } else {
              queryPath.push(betweenArray);
            }
          }
        }
        /**
         * Logic for loopback `inq`(include) filter of where
         * @example {where: { property: { inq: [val1, val2, ...]}}}
         */
        if (spec === 'inq') {
          let inArray = {
            terms: {}
          };
          inArray.terms[key] = cond;
          // Additional handling for nested Objects
          if (isNestedKey) {
            inArray = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: inArray
              }
            };
          }
          if (root) {
            queryPath.bool.must.push(inArray);
          } else {
            queryPath.push(inArray);
          }
          log('ESConnector.prototype.buildDeepNestedQueries',
            'body', body,
            'inArray', JSON.stringify(inArray, null, 0));
        }

        /**
         * Logic for loopback `nin`(not include) filter of where
         * @example {where: { property: { nin: [val1, val2, ...]}}}
         */
        if (spec === 'nin') {
          let notInArray = {
            terms: {}
          };
          notInArray.terms[key] = cond;
          // Additional handling for nested Objects
          if (isNestedKey) {
            notInArray = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: {
                  bool: {
                    must: [notInArray]
                  }
                }
              }
            };
          }
          if (root) {
            queryPath.bool.must_not.push(notInArray);
          } else {
            queryPath.push({
              bool: {
                must_not: [notInArray]
              }
            });
          }
        }

        /**
         * Logic for loopback `neq` (not equal) filter of where
         * @example {where: {role: {neq: 'lead' }}}
         */
        if (spec === 'neq') {
          /**
           * First - filter the documents where the given property exists
           * @type {{exists: {field: *}}}
           */
          // var missingFilter = {exists :{field : key}};
          /**
           * Second - find the document where value not equals the given value
           * @type {{term: {}}}
           */
          let notEqual = {
            term: {}
          };
          notEqual.term[key] = cond;
          /**
           * Apply the given filter in the main filter(body) and on given path
           */
          // Additional handling for nested Objects
          if (isNestedKey) {
            notEqual = {
              match: {}
            };
            notEqual.match[key] = cond;
            notEqual = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: {
                  bool: {
                    must: [notEqual]
                  }
                }
              }
            };
          }
          if (root) {
            queryPath.bool.must_not.push(notEqual);
          } else {
            queryPath.push({
              bool: {
                must_not: [notEqual]
              }
            });
          }


          // body.query.bool.must.push(missingFilter);
        }
        // TODO: near - For geolocations, return the closest points,
        // ...sorted in order of distance.  Use with limit to return the n closest points.
        // TODO: like, nlike
        // TODO: ilike, inlike
        if (spec === 'like') {
          let likeQuery = {
            regexp: {}
          };
          likeQuery.regexp[key] = cond;

          // Additional handling for nested Objects
          if (isNestedKey) {
            likeQuery = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: {
                  bool: {
                    must: [likeQuery]
                  }
                }
              }
            };
          }
          if (root) {
            queryPath.bool.must.push(likeQuery);
          } else {
            queryPath.push(likeQuery);
          }
        }

        if (spec === 'nlike') {
          let nlikeQuery = {
            regexp: {}
          };
          nlikeQuery.regexp[key] = cond;

          // Additional handling for nested Objects
          if (isNestedKey) {
            nlikeQuery = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: {
                  bool: {
                    must_not: [nlikeQuery]
                  }
                }
              }
            };
          }
          if (root) {
            queryPath.bool.must_not.push(nlikeQuery);
          } else {
            queryPath.push({
              bool: {
                must_not: [nlikeQuery]
              }
            });
          }
        }
        // TODO: regex

        // geo_shape || geo_distance || geo_bounding_box
        if (spec === 'geo_shape' || spec === 'geo_distance' || spec === 'geo_bounding_box') {
          let geoQuery = {
            filter: {}
          };
          geoQuery.filter[spec] = cond;

          if (isNestedKey) {
            geoQuery = {
              nested: {
                path: nestedSuperKey,
                score_mode: 'max',
                query: {
                  bool: geoQuery
                }
              }
            };
            if (root) {
              queryPath.bool.must.push(geoQuery);
            } else {
              queryPath.push(geoQuery);
            }
          } else if (root) {
            queryPath.bool.filter = geoQuery;
          } else {
            queryPath.push({
              bool: geoQuery
            });
          }
        }
      } else {
        let nestedQuery = {};
        if (typeof value === 'string') {
          value = value.trim();
          if (value.indexOf(' ') > -1) {
            nestedQuery.match_phrase = {};
            nestedQuery.match_phrase[key] = value;
          } else {
            nestedQuery.match = {};
            nestedQuery.match[key] = value;
          }
        } else {
          nestedQuery.match = {};
          nestedQuery.match[key] = value;
        }
        // Additional handling for nested Objects
        if (isNestedKey) {
          nestedQuery = {
            nested: {
              path: nestedSuperKey,
              score_mode: 'max',
              query: {
                bool: {
                  must: [nestedQuery]
                }
              }
            }
          };
        }

        if (root) {
          queryPath.bool.must.push(nestedQuery);
        } else {
          queryPath.push(nestedQuery);
        }

        log('ESConnector.prototype.buildDeepNestedQueries',
          'body', body,
          'nestedQuery', JSON.stringify(nestedQuery, null, 0));
      }
    }
  });
};

/**
 * Get document Id validating data
 * @param {String} id
 * @returns {Number} Id
 * @constructor
 */
ESConnector.prototype.getDocumentId = (id) => {
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

ESConnector.prototype.getDefaultIdType = () => String;

/**
 * Create a new model instance
 * @param {String} model name
 * @param {object} data info
 * @param {Function} done - invoke the callback with the created model's id as an argument
 */
ESConnector.prototype.create = (model, data, done) => {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.create', model, data);
  }

  const idValue = self.getIdValue(model, data);
  const idName = self.idName(model);
  log('ESConnector.prototype.create', 'idName', idName, 'idValue', idValue);
  /* TODO: If model has custom id with generated false and
    if Id field is not prepopulated then create should fail.
  */
  /* TODO: If model Id is not string and generated is true then
    create should fail because the auto generated es id is of type string and we cannot change it.
  */
  const document = self.addDefaults(model, 'create');
  document[self.idField] = self.getDocumentId(idValue);
  document.body = {};
  _.assign(document.body, data);
  log('ESConnector.prototype.create', 'document', document);
  let method = 'create';
  if (!document[self.idField]) {
    method = 'index'; // if there is no/empty id field, we must use the index method to create it (API 5.0)
  }
  document.body.docType = model;
  self.db[method](
    document
  ).then(
    (response) => {
      log('ESConnector.prototype.create', 'response', response);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.create', 'will invoke callback with id:', response._id);
      // eslint-disable-next-line no-underscore-dangle
      done(null, response._id); // the connector framework expects the id as a return value
    }
  ).catch((err) => {
    log('ESConnector.prototype.create', err.message);
    return done(err, null);
  });
};

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
ESConnector.prototype.all = (model, filter, done) => {
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
};

/**
 * Delete model instances by query
 * @param {String} modelName name
 * @param {String} whereClause criteria
 * @param {Function} cb callback
 */
ESConnector.prototype.destroyAll = (modelName, whereClause, cb) => {
  const self = this;

  if ((!cb) && _.isFunction(whereClause)) {
    cb = whereClause;
    whereClause = {};
  }
  log('ESConnector.prototype.destroyAll', 'modelName', modelName, 'whereClause', JSON.stringify(whereClause, null, 0));

  const idName = self.idName(modelName);
  const body = {
    query: self.buildWhere(modelName, idName, whereClause).query
  };

  const defaults = self.addDefaults(modelName, 'destroyAll');
  const options = _.defaults({
    body
  }, defaults);
  log('ESConnector.prototype.destroyAll', 'options:', JSON.stringify(options, null, 2));
  self.db.deleteByQuery(options)
    .then((response) => {
      cb(null, response);
    })
    .catch((err) => {
      log('ESConnector.prototype.destroyAll', err.message);
      return cb(err, null);
    });
};

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
ESConnector.prototype.updateAll = (model, where, data, options, cb) => {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.updateAll', 'model', model, 'options', options, 'where', where, 'date', data);
  }
  const idName = self.idName(model);
  log('ESConnector.prototype.updateAll', 'idName', idName);

  const defaults = self.addDefaults(model, 'updateAll');

  const body = {
    query: self.buildWhere(model, idName, where).query
  };

  body.script = {
    inline: '',
    params: {}
  };
  _.forEach(data, (value, key) => {
    if (key !== '_id' || key !== idName) {
      // default language for inline scripts is painless if ES 5, so this needs the extra params.
      body.script.inline += `ctx._source.${key}=params.${key};`;
      body.script.params[key] = value;
      if (key === 'docType') {
        body.script.params[key] = model;
      }
    }
  });

  const document = _.defaults({
    body
  }, defaults);
  log('ESConnector.prototype.updateAll', 'document to update', document);

  self.db.updateByQuery(document)
    .then((response) => {
      log('ESConnector.prototype.updateAll', 'response', response);
      return cb(null, {
        updated: response.updated,
        total: response.total
      });
    }, (err) => {
      log('ESConnector.prototype.updateAll', err.message);
      return cb(err, null);
    });
};

ESConnector.prototype.update = ESConnector.prototype.updateAll;

/**
 * Count model instances by query
 * @param {String} model name
 * @param {String} where criteria
 * @param {Function} done callback
 */
ESConnector.prototype.count = (modelName, done, where) => {
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
};

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
ESConnector.prototype.find = (modelName, id, done) => {
  const self = this;
  log('ESConnector.prototype.find', 'model', modelName, 'id', id);

  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const defaults = self.addDefaults(modelName, 'find');
  self.db.get(_.defaults({
    id: self.getDocumentId(id)
  }, defaults)).then(
    (response) => {
      done(null, self.dataSourceToModel(modelName, response));
    },
    (err) => {
      log('ESConnector.prototype.find', err.message);
      return done(err, null);
    }
  );
};

/**
 * Delete a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {Function} done callback
 */
ESConnector.prototype.destroy = (modelName, id, done) => {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.destroy', 'model', modelName, 'id', id);
  }

  const filter = self.addDefaults(modelName, 'destroy');
  filter[self.idField] = self.getDocumentId(id);
  if (!filter[self.idField]) {
    throw new Error('Document id not setted!');
  }
  self.db.delete(
    filter
  ).then(
    (response) => {
      done(null, response);
    },
    (err) => {
      log('ESConnector.prototype.destroy', err.message);
      return done(err, null);
    }
  );
};

/**
 * Update a model instance by id
 *
 */

ESConnector.prototype.updateAttributes = (modelName, id, data, callback) => {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.updateAttributes', 'modelName', modelName, 'id', id, 'data', data);
  }
  const idName = self.idName(modelName);
  log('ESConnector.prototype.updateAttributes', 'idName', idName);

  const defaults = self.addDefaults(modelName, 'updateAll');

  const body = {
    query: self.buildWhere(modelName, idName, {
      _id: id
    }).query
  };

  body.script = {
    inline: '',
    params: {}
  };
  _.forEach(data, (value, key) => {
    if (key !== '_id' || key !== idName) {
      // default language for inline scripts is painless if ES 5, so this needs the extra params.
      body.script.inline += `ctx._source.${key}=params.${key};`;
      body.script.params[key] = value;
      if (key === 'docType') {
        body.script.params[key] = modelName;
      }
    }
  });

  const document = _.defaults({
    body
  }, defaults);
  log('ESConnector.prototype.updateAttributes', 'document to update', document);

  self.db.updateByQuery(document)
    .then((response) => {
      log('ESConnector.prototype.updateAttributes', 'response', response);
      return callback(null, {
        updated: response.updated,
        total: response.total
      });
    }, (err) => {
      log('ESConnector.prototype.updateAttributes', err.message);
      return callback(err, null);
    });
};

/**
 * Check existence of a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {function} done callback
 */
ESConnector.prototype.exists = (modelName, id, done) => {
  const self = this;
  log('ESConnector.prototype.exists', 'model', modelName, 'id', id);

  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const defaults = self.addDefaults(modelName, 'exists');
  self.db.exists(_.defaults({
    id: self.getDocumentId(id)
  }, defaults)).then(
    (exists) => {
      done(null, exists);
    },
    (err) => {
      log('ESConnector.prototype.exists', err.message);
      return done(err, null);
    }
  );
};

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
// eslint-disable-next-line consistent-return
ESConnector.prototype.save = (model, data, done) => {
  const self = this;
  if (self.debug) {
    log('ESConnector.prototype.save ', 'model', model, 'data', data);
  }

  const idName = self.idName(model);
  const defaults = self.addDefaults(model, 'save');
  const id = self.getDocumentId(data[idName]);

  if (id === undefined || id === null) {
    return done('Document id not setted!', null);
  }
  data.docType = model;
  self.db.update(_.defaults({
    id,
    body: {
      doc: data,
      doc_as_upsert: false
    }
  }, defaults)).then((response) => {
    done(null, response);
  }, (err) => done(err, null));
};

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
ESConnector.prototype.updateOrCreate = (modelName, data, callback) => {
  const self = this;
  log('ESConnector.prototype.updateOrCreate', 'modelName', modelName, 'data', data);

  const idName = self.idName(modelName);
  const id = self.getDocumentId(data[idName]);
  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const defaults = self.addDefaults(modelName, 'updateOrCreate');
  data.docType = modelName;
  self.db.update(_.defaults({
    id,
    body: {
      doc: data,
      doc_as_upsert: true
    }
  }, defaults)).then(
    (response) => {
      /**
       * In the case of an update, elasticsearch only provides a confirmation that it worked
       * but does not provide any model data back. So what should be passed back in
       * the data object (second argument of callback)?
       *   Q1) Should we just pass back the data that was meant to be updated
       *       and came in as an argument to the updateOrCreate() call? This is what
       *       the memory connector seems to do.
       *       A: [Victor Law] Yes, that's fine to do. The reason why we are passing the data there
       *       and back is to support databases that can add default values to undefined properties,
       *       typically the id property is often generated by the backend.
       *   Q2) OR, should we make an additional call to fetch the data for that id internally,
       *       within updateOrCreate()? So we can make sure to pass back a data object?
       *       A: [Victor Law]
       *          - Most connectors don't fetch the inserted/updated data
       *            and hope the data stored into DB
       *            will be the same as the data sent to DB for create/update.
       *          - It's true in most cases but not always. For example, the DB might have triggers
       *            that change the value after the insert/update.
       *            - We don't support that yet.
       *            - In the future, that can be controlled via an options property,
       *              such as fetchNewInstance = true.
       *
       * NOTE: Q1 based approach has been implemented for now.
       */
      // eslint-disable-next-line no-underscore-dangle
      if (response._version === 1) { // distinguish if it was an update or create operation in ES
        // eslint-disable-next-line no-underscore-dangle
        data[idName] = response._id;
        // eslint-disable-next-line no-underscore-dangle
        log('ESConnector.prototype.updateOrCreate', 'assigned ID', idName, '=', response._id);
      }
      callback(null, data, {
        isNewInstance: response.created
      });
    },
    (err) => {
      log('ESConnector.prototype.updateOrCreate', err.message);
      return callback(err, null);
    }
  );
};

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
ESConnector.prototype.replaceOrCreate = (modelName, data, callback) => {
  const self = this;
  log('ESConnector.prototype.replaceOrCreate', 'modelName', modelName, 'data', data);

  const idName = self.idName(modelName);
  const id = self.getDocumentId(data[idName]);
  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  const document = self.addDefaults(modelName, 'replaceOrCreate');
  document[self.idField] = id;
  document.body = {};
  _.assign(document.body, data);
  document.body.docType = modelName;
  log('ESConnector.prototype.replaceOrCreate', 'document', document);
  self.db.index(
    document
  ).then(
    (response) => {
      log('ESConnector.prototype.replaceOrCreate', 'response', response);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.replaceOrCreate', 'will invoke callback with id:', response._id);
      // eslint-disable-next-line no-underscore-dangle
      callback(null, response._id); // the connector framework expects the id as a return value
    }
  ).catch((err) => {
    log('ESConnector.prototype.replaceOrCreate', err.message);
    return callback(err, null);
  });
};

ESConnector.prototype.replaceById = (modelName, id, data, options, callback) => {
  const self = this;
  log('ESConnector.prototype.replaceById', 'modelName', modelName, 'id', id, 'data', data);

  const idName = self.idName(modelName);
  if (id === undefined || id === null) {
    throw new Error('id not set!');
  }

  // eslint-disable-next-line no-underscore-dangle
  const modelProperties = this._models[modelName].properties;

  const document = self.addDefaults(modelName, 'replaceById');
  document[self.idField] = self.getDocumentId(id);
  document.body = {};
  _.assign(document.body, data);
  document.body.docType = modelName;
  if (Object.prototype.hasOwnProperty.call(modelProperties, idName)) {
    document.body[idName] = id;
  }
  log('ESConnector.prototype.replaceById', 'document', document);
  self.db.index(
    document
  ).then(
    (response) => {
      log('ESConnector.prototype.replaceById', 'response', response);
      // eslint-disable-next-line no-underscore-dangle
      log('ESConnector.prototype.replaceById', 'will invoke callback with id:', response._id);
      // eslint-disable-next-line no-underscore-dangle
      callback(null, response._id); // the connector framework expects the id as a return value
    }
  ).catch((err) => {
    log('ESConnector.prototype.replaceById', err.message);
    return callback(err, null);
  });
};

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
ESConnector.prototype.automigrate = require('./automigrate.js')({
  log,
  lodash: _
});

module.exports.name = ESConnector.name;
module.exports.ESConnector = ESConnector;
