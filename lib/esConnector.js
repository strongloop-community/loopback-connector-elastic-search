'use strict';

var util = require('util');
var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var Promise = require('bluebird');

var log = require('debug')('loopback:connector:elasticsearch');

var elasticsearch = require('elasticsearch');
var deleteByQuery;
var Connector = require('loopback-connector').Connector;

/*eslint no-console: ["error", { allow: ["trace"] }] */

/**
 * Initialize connector with datasource, configure settings and return
 * @param {object} dataSource
 * @param {function} done callback
 */
module.exports.initialize = function (dataSource, callback) {
    if (!elasticsearch) {
        return;
    }

    var settings = dataSource.settings || {};

    dataSource.connector = new ESConnector(settings, dataSource);

    if (callback) {
        dataSource.connector.connect(callback);
    }
};

/**
 * Connector constructor
 * @param {object} datasource settings
 * @param {object} dataSource
 * @constructor
 */
var ESConnector = function (settings, dataSource) {
    Connector.call(this, 'elasticsearch', settings);

    this.searchIndex = settings.index || '';
    this.searchIndexSettings = settings.settings || {};
    this.searchType = settings.type || '';
    this.defaultSize = (settings.defaultSize || 10);
    this.idField = 'id';

    this.debug = settings.debug || log.enabled;
    if (this.debug) {
        log('Settings: %j', settings);
    }

    this.dataSource = dataSource;
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
    var config = {
        hosts: this.settings.hosts || {host:'127.0.0.1',port:9200},
        requestTimeout: this.settings.requestTimeout,
        apiVersion: this.settings.apiVersion,
        log: this.settings.log || 'error',
        suggestCompression: true
    };

    if (this.settings.amazonES) {
        config.connectionClass = require('http-aws-es');
        config.amazonES = this.settings.amazonES || {
            region: 'us-east-1',
            accessKey: 'AKID',
            secretKey: 'secret'
        }
    }

    if (this.settings.ssl) {
        config.ssl = {
            ca: (this.settings.ssl.ca) ? fs.readFileSync(path.join(__dirname, this.settings.ssl.ca)) : fs.readFileSync(path.join(__dirname, '..', 'cacert.pem')),
            rejectUnauthorized: this.settings.ssl.rejectUnauthorized || true
        };
    }
    // Note: http://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
    //       Due to the complex nature of the configuration, the config object you pass in will be modified
    //       and can only be used to create one Client instance.
    //       Related Github issue: https://github.com/elasticsearch/elasticsearch-js/issues/33
    //       Luckily getClientConfig() pretty much clones settings so we shouldn't have to worry about it.
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
    var self = this;
    if (self.db) {
        process.nextTick(function () {
            callback && callback(null, self.db);
        });
    }
    else {
        self.db = new elasticsearch.Client(self.getClientConfig());
        if (self.settings.apiVersion.indexOf('2') === 0) {
            log('injecting deleteByQuery');
            deleteByQuery = require('elastic-deletebyquery');
            deleteByQuery(self.db);
            self.db.deleteByQuery = Promise.promisify(self.db.deleteByQuery);
        }

        // NOTE: any & all indices and mappings will be created or their existence verified before proceeding
        if(self.settings.mappings) {
          self.setupMappings()
              .then(function(){
                log('ESConnector.prototype.connect', 'setupMappings', 'finished');
                callback && callback(null, self.db);
              })
              .catch(function(err){
                log('ESConnector.prototype.connect', 'setupMappings', 'failed', err);
                callback && callback(err);
              });
        }
        else {
          process.nextTick(function () {
            callback && callback(null, self.db);
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
    var self = this;
    var db = self.db;
    var settings = self.settings;
    if (_.isFunction(modelNames)) {
        callback = modelNames;
        modelNames = _.pluck(settings.mappings, 'name');
    }
    log('ESConnector.prototype.removeMappings', 'modelNames', modelNames);

    var mappingsToRemove = _.filter(settings.mappings, function(mapping){
        return !modelNames || _.includes(modelNames, mapping.name);
    });
    log('ESConnector.prototype.removeMappings', 'mappingsToRemove', _.pluck(mappingsToRemove, 'name'));

    Promise.map(
        mappingsToRemove,
        function (mapping) {
            var defaults = self.addDefaults(mapping.name);
            log('ESConnector.prototype.removeMappings', 'calling self.db.indices.existsType()');
            return db.indices.existsType(defaults).then(function(exists) {
                if (!exists) return Promise.resolve();
                log('ESConnector.prototype.removeMappings', 'calling self.db.indices.deleteMapping()');
                return db.indices.deleteMapping(defaults)
                    .then(function (body) {
                        log('ESConnector.prototype.removeMappings', mapping.name, body);
                        return Promise.resolve();
                    },
                    function (err) {
                        console.trace(err.message);
                        return Promise.reject(err);
                    });
            }, function(err) {
                console.trace(err.message);
                return Promise.reject(err);
            });
        },
        {concurrency: 1}
    )
        .then(function () {
            log('ESConnector.prototype.removeMappings', 'finished');
            callback(null, self.db); // TODO: what does the connector framework want back as arguments here?
        })
        .catch(function(err){
            log('ESConnector.prototype.removeMappings', 'failed');
            callback(err);
        });
};

ESConnector.prototype.setupMappings = require('./setupMappings.js')({
  log: log
  ,lodash: _
  ,bluebird: Promise
});

ESConnector.prototype.setupMapping = require('./setupMapping.js')({
  log: log
  ,lodash: _
  ,bluebird: Promise
});

ESConnector.prototype.setupIndex = require('./setupIndex.js')({
  log: log
  ,bluebird: Promise
});



/**
 * Ping to test elastic connection
 * @returns {String} with ping result
 */
ESConnector.prototype.ping = function (cb) {
    this.db.ping({
        requestTimeout : 1000
    }, function (error) {
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
            return new Array();
        }
        else {
            return value;
        }
    } else if (property.type === String) {
        return value.toString();
    } else if (property.type === Number) {
        return Number(value);
    } else if (property.type === Date){
        return new Date(value);
    } else {
        return value;
    }
};

/**
 * Match and transform data structure to modelName
 * @param {String} modelName name
 * @param {Object} data from DB
 * @returns {object} modeled document
 */
ESConnector.prototype.matchDataToModel = function (modelName, data, esId, idName) {
    //log('ESConnector.prototype.matchDataToModel', 'modelName', modelName, 'data', JSON.stringify(data,null,0));
    var self = this;
    if (!data) {
        return null;
    }
    try {
        var document = {};

        var properties = this._models[modelName].properties;
        _.assign(document, data); // it can't be this easy, can it?
        document[idName] = esId;

        for (var propertyName in properties) {
            var propertyValue = data[propertyName];
            // log('ESConnector.prototype.matchDataToModel', propertyName, propertyValue);
            if (propertyValue!==undefined && propertyValue!==null) {
                document[propertyName] = self.getValueFromProperty(properties[propertyName], propertyValue);
            }
        }
        log('ESConnector.prototype.matchDataToModel', 'document', JSON.stringify(document,null,0));
        return document;
    } catch (err) {
        console.trace(err.message);
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
    log('ESConnector.prototype.dataSourceToModel', 'modelName', modelName, 'data', JSON.stringify(data,null,0));

    //return data._source; // TODO: super-simplify?
    return this.matchDataToModel(modelName, data._source, data._id, idName);
};

/**
 * Add defaults such as index name and type
 *
 * @param {String} modelName
 * @returns {object} Filter with index and type
 */
ESConnector.prototype.addDefaults = function (modelName) {
    var self = this;
    log('ESConnector.prototype.addDefaults', 'modelName:', modelName);

    //TODO: fetch index and type from `self.settings.mappings` too
    var indexFromDatasource, typeFromDatasource;
    var mappingFromDatasource = _.find(self.settings.mappings,
        function(mapping) {
          return mapping.name === modelName;
        });
    if (mappingFromDatasource) {
      indexFromDatasource = mappingFromDatasource.index;
      typeFromDatasource = mappingFromDatasource.type;
    }

    var filter = {};
    if (this.searchIndex) {
        filter.index = indexFromDatasource || this.searchIndex;
    }
    filter.type = typeFromDatasource || this.searchType || modelName;

    var modelClass = this._models[modelName];
    if (modelClass && _.isObject(modelClass.settings.elasticsearch)) {
        _.extend(filter, modelClass.settings.elasticsearch);
    }

    log('ESConnector.prototype.addDefaults', 'filter:', filter);
    return filter;
};

/**
 * Make filter from criteria, data index and type
 * Ex:
 *   {"body": {"query": {"match": {"title": "Futuro"}}}}
 *   {"q" : "Futuro"}
 * @param {String} modelName filter
 * @param {String} criteria filter
 * @param {number} size of rows to return, if null then skip
 * @param {number} offset to return, if null then skip
 * @returns {object} filter
 */
ESConnector.prototype.buildFilter = function (modelName, idName, criteria, size, offset) {
    var self = this;
    log('ESConnector.prototype.buildFilter', 'model', modelName, 'idName', idName,
        'criteria', JSON.stringify(criteria,null,0));

    if (idName===undefined || idName===null) {
        throw new Error('idName not set!');
    }

    var filter = this.addDefaults(modelName);
    filter.body = {};

    if (size!==undefined && size!==null) {
        filter.size = size;
    }
    if (offset!==undefined && offset!==null) {
        filter.from = offset;
    }

    if (criteria) {
        // `criteria` is set by app-devs, therefore, it overrides any connector level arguments
        if (criteria.limit!==undefined && criteria.limit!==null) {
            filter.size = criteria.limit;
        }
        if (criteria.skip!==undefined && criteria.skip!==null)
        {
            filter.from = criteria.skip;
        }
        else if (criteria.offset!==undefined && criteria.offset!==null) { // use offset as an alias for skip
            filter.from = criteria.offset;
        }
        if(criteria.fields) {
            // { fields: {propertyName: <true|false>, propertyName: <true|false>, ... } }
            //filter.body.fields = self.buildOrder(model, idName, criteria.fields);
            // TODO: make it so
            // http://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-request-source-filtering.html
            // http://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-request-fields.html
            /*POST /shakespeare/User/_search
            {
                "_source": {
                    "include": ["seq"],
                    "exclude": ["seq"]
                }
            }*/

            /* @raymondfeng and @bajtos - I'm observing something super strange,
            i haven't implemented the FIELDS filter for elasticsearch connector
            but the test which should fail until I implement such a feature ... is actually passing!
            ... did someone at some point of time implement an in-memory filter for FIELDS
            in the underlying loopback-connector implementation? */
        }
        if(criteria.order) {
            log('ESConnector.prototype.buildFilter', 'will delegate sorting to buildOrder()');
            filter.body.sort = self.buildOrder(modelName, idName, criteria.order);
        }
        else { // TODO: expensive~ish and no clear guidelines so turn it off?
            //var idNames = this.idNames(model); // TODO: support for compound ids?
            var modelProperties = this._models[modelName].properties;
            if (idName === 'id' && modelProperties.id.generated) {
                //filter.body.sort = ['_id']; // requires mapping to contain: '_id' : {'index' : 'not_analyzed','store' : true}
                log('ESConnector.prototype.buildFilter', 'will sort on _uid by default when IDs are meant to be auto-generated by elasticsearch');
                filter.body.sort = ['_uid'];
            } else {
                log('ESConnector.prototype.buildFilter', 'will sort on loopback specified IDs');
                filter.body.sort = [idName]; // default sort should be based on fields marked as id
            }
        }
        if (criteria.where) {
            filter.body.query = self.buildWhere(modelName, idName, criteria.where).query;
        }
        // TODO: Include filter
        else if (criteria.suggests) { // TODO: remove HACK!!!
            filter.body = criteria.suggests; // assume that the developer has provided ES compatible DSL
        }
        else if (criteria.native) {
            filter.body = criteria.native; // assume that the developer has provided ES compatible DSL
        }
        else if (_.keys(criteria).length===0) {
            filter.body = {
                'query': {
                    'match_all': {}
                }
            };
        }
    }

    log('ESConnector.prototype.buildFilter', 'constructed', JSON.stringify(filter,null,0));
    return filter;
};

/**
 * 1. Words of wisdom from @doublemarked:
 *    > When writing a query without an order specified, the author should not assume any reliable order.
 *    > So if we’re not assuming any order, there is not a compelling reason to potentially slow down
 *    > the query by enforcing a default order.
 * 2. Yet, most connector implementations do enforce a default order ... what to do?
 *
 * @param model
 * @param idName
 * @param order
 * @returns {Array}
 */
ESConnector.prototype.buildOrder = function (model, idName, order) {
    var sort = [];

    var keys = order;
    if (typeof keys === 'string') {
        keys = keys.split(',');
    }
    for (var index = 0, len = keys.length; index < len; index++) {
        var m = keys[index].match(/\s+(A|DE)SC$/);
        var key = keys[index];
        key = key.replace(/\s+(A|DE)SC$/, '').trim();
        if(key === 'id' || key === idName) {
            key = '_uid';
        }
        if (m && m[1] === 'DE') {
            //sort[key] = -1;
            var temp  = {};
            temp[key] = 'desc';
            sort.push(temp);
        } else {
            //sort[key] = 1;
            sort.push(key);
        }
    }

    return sort;
};

ESConnector.prototype.buildWhere = function (model, idName, where) {
    var self = this;
    log('ESConnector.prototype.buildWhere', 'model', model, 'idName', idName, 'where', JSON.stringify(where,null,0));

    var body = {
        query: {
            bool: {
                must: [],
                should: [],
                'must_not': []
            }
        }
    };

    self.buildNestedQueries(body, model, idName, where);

    if(body && body.query && body.query.bool && body.query.bool.must && body.query.bool.must.length === 0) {
        delete body.query.bool['must']; // jshint ignore:line
    }
    if(body && body.query && body.query.bool && body.query.bool.should && body.query.bool.should.length === 0) {
        delete body.query.bool['should']; // jshint ignore:line
    }
    if(body && body.query && body.query.bool && body.query.bool.must_not && body.query.bool.must_not.length === 0) { // jshint ignore:line
        delete body.query.bool['must_not']; // jshint ignore:line
    }
    return body;
};

ESConnector.prototype.buildNestedQueries = function (body, model, idName, where, parentOperator) {
    var self = this;
    log('ESConnector.prototype.buildNestedQueries',
        'model',  model, 'idName', idName,
        '\nbody', JSON.stringify(body,null,0),
        '\nwhere',  JSON.stringify(where,null,0),
        '\nparentOperator', parentOperator);

    if (_.keys(where).length===0) {
        body.query = {
            'match_all': {}
        };
        log('ESConnector.prototype.buildNestedQueries',
            '\nbody', JSON.stringify(body,null,0));
    }
    else {
        _.forEach(where, function(value, key) {
            var cond = value;
            if(key === 'id' || key === idName) {
                //where.match._id = value;
                key = '_id';
            }

            // TODO: or	Logical OR operator
            // TODO: and Logical AND operator
            //   {"where":{"and":[{"id":{"inq":["1","2","3","4"]}},{"vip":true}]}}
            if (key === 'and' || key === 'or' || key === 'nor') {
                if (Array.isArray(cond)) {
                    cond = cond.map(function (c) {
                        self.buildNestedQueries(body, model, idName, c, key);
                        log('ESConnector.prototype.buildNestedQueries',
                            'mapped', 'body', JSON.stringify(body,null,0));
                    });
                }
                // TODO: distinguish between and/or to populate must/should
                /*query['$' + key ] = cond;
                 delete query[key];*/
                return;
            }

            var spec = false;
            var options = null;
            if (cond && cond.constructor.name === 'Object') {
                options = cond.options;
                spec = Object.keys(cond)[0];
                cond = cond[spec];
            }
            log('ESConnector.prototype.buildNestedQueries',
                'spec', spec, 'key', key, 'cond', JSON.stringify(cond,null,0), 'options', options);
            if (spec) {
                if (spec === 'gte' || spec === 'gt' || spec === 'lte' || spec === 'lt') {
                    var rangeQuery = {range:{}};
                    var rangeQueryGuts = {};
                    rangeQueryGuts[spec] = cond;
                    rangeQuery.range[key] = rangeQueryGuts;
                    self.mergeNestedQueryWithParentOperator(body, rangeQuery, parentOperator);
                }
                // TODO: between - True if the value is between the two specified values: greater than or equal to first value and less than or equal to second value.
                // TODO: inq, nin - In / not in an array of values.
                if (spec === 'inq') {
                    cond.map(function (x) {
                        var nestedQuery = {match:{}};
                        nestedQuery.match[key] = x;
                        log('ESConnector.prototype.buildNestedQueries',
                            'nestedQuery', JSON.stringify(nestedQuery,null,0));
                        body.query.bool.should.push(nestedQuery);
                    });
                }
                // TODO: near - For geolocations, return the closest points, sorted in order of distance.  Use with limit to return the n closest points.
                // TODO: neq - Not equal (!=)
                // TODO: like, nlike
            }
            else {
                var nestedQuery = {match:{}};
                nestedQuery.match[key] = value;
                log('ESConnector.prototype.buildNestedQueries',
                    'parentOperator', parentOperator,
                    'nestedQuery', JSON.stringify(nestedQuery,null,0));
                self.mergeNestedQueryWithParentOperator(body, nestedQuery, parentOperator);
                /*if(parentOperator === 'and'){
                    body.query.bool.must.push(nestedQuery);
                }
                else if(parentOperator === 'or'){
                    body.query.bool.should.push(nestedQuery);
                }
                else if(parentOperator === 'nor'){
                    body.query.bool.must_not.push(nestedQuery); // jshint ignore:line
                }
                else {
                    body.query.bool.must.push(nestedQuery);
                }*/
            }
        });
    }
};

ESConnector.prototype.mergeNestedQueryWithParentOperator = function (body, nestedQuery, parentOperator) {
    if(parentOperator === 'and'){
        body.query.bool.must.push(nestedQuery);
    }
    else if(parentOperator === 'or'){
        body.query.bool.should.push(nestedQuery);
    }
    else if(parentOperator === 'nor'){
        body.query.bool.must_not.push(nestedQuery); // jshint ignore:line
    }
    else {
        body.query.bool.must.push(nestedQuery);
    }
};

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
        } else {
            return id;
        }
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
 *   > very useful for setting a default type for IDs like "string" rather than "number"
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
ESConnector.prototype.create = function (model, data, done) {
    var self = this;
    if (self.debug) {
        log('ESConnector.prototype.create', model, data);
    }

    var idValue = self.getIdValue(model, data);
    var idName = self.idName(model);
    log('ESConnector.prototype.create', 'idName', idName, 'idValue', idValue);
    //TODO: If model has custom id with generated false and if Id field is not prepopulated then create should fail.
    //TODO: If model Id is not string and generated is true then create should fail because the auto generated es id is of type string and we cannot change it.
    var document = self.addDefaults(model);
    document[self.idField] = self.getDocumentId(idValue);
    document.body = {};
    _.assign(document.body, data);
    log('ESConnector.prototype.create', 'document', document);

    var method = 'create';
    if(!document[self.idField]) {
        method = 'index'; // if there is no/empty id field, we must use the index method to create it (API 5.0)
    }
    self.db[method](
        document
    ).then(
        function (response) {
            log('ESConnector.prototype.create', 'response', response);
            log('ESConnector.prototype.create', 'will invoke callback with id:', response._id);
            done(null, response._id); // the connector framework expects the id as a return value
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return done(err, null);
            }
        }
    );
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
 * make sure to map "_id" into the content, just in case its an auto-generated one.
 */
ESConnector.prototype.all = function all(model, filter, done) {
    var self = this;
    log('ESConnector.prototype.all', 'model', model, 'filter', JSON.stringify(filter,null,0));

    var idName = self.idName(model);
    log('ESConnector.prototype.all', 'idName', idName);

    self.db.search(
        self.buildFilter(model, idName, filter, self.defaultSize)
    ).then(
        function (body) {
            var result = [];
            body.hits.hits.forEach(function (item) {
                result.push(self.dataSourceToModel(model, item, idName));
            });
            log('ESConnector.prototype.all', 'model', model, 'result', JSON.stringify(result,null,2));
            done(null, result);
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return done(err, null);
            }
        }
    );
};

/**
 * Delete model instances by query
 * @param {String} modelName name
 * @param {String} whereClause criteria
 * @param {Function} cb callback
 */
ESConnector.prototype.destroyAll = function destroyAll(modelName, whereClause, cb) {
    var self = this;

    if ((!cb) && _.isFunction(whereClause)) {
        cb = whereClause;
        whereClause = {};
    }
    log('ESConnector.prototype.destroyAll', 'modelName', modelName, 'whereClause', JSON.stringify(whereClause,null,0));

    var idName = self.idName(modelName);
    var body = {
        query: self.buildWhere(modelName, idName, whereClause).query
    };

    var defaults = self.addDefaults(modelName);

    defaults.refresh = true;

    var options = _.defaults({ body: body }, defaults);
    log('ESConnector.prototype.destroyAll', 'options:', JSON.stringify(options,null,2));
    self.db.deleteByQuery(options)
        .then(function(response){
            cb(null, response);
        })
        .catch(function(err) {
            console.trace(err.message);
            return cb(err, null);
        });
};

/**
 * Update model instances by query
 *
 * NOTES:
 * > Without an update by query plugin, this isn't supported by ES out-of-the-box
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
//ESConnector.prototype.updateAll = function updateAll(model, where, data, cb) {...};
//ESConnector.prototype.update = ESConnector.prototype.updateAll;

/**
 * Count model instances by query
 * @param {String} model name
 * @param {String} where criteria
 * @param {Function} done callback
 */
ESConnector.prototype.count = function count(modelName, done, where) {
    var self = this;
    log('ESConnector.prototype.count', 'model', modelName, 'where', where);

    var idName = self.idName(modelName);
    var body = where.native ? where.native : {
        query: self.buildWhere(modelName, idName, where).query
    };

    var defaults = self.addDefaults(modelName);
    self.db.count(_.defaults({ body: body }, defaults)).then(
        function (response) {
            done(null, response.count);
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return done(err, null);
            }
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
ESConnector.prototype.find = function find(modelName, id, done) {
    var self = this;
    log('ESConnector.prototype.find', 'model', modelName, 'id', id);

    if (id===undefined || id===null) {
        throw new Error('id not set!');
    }

    var defaults = self.addDefaults(modelName);
    self.db.get(_.defaults({
        id: self.getDocumentId(id)
    }, defaults)).then(
        function (response) {
            done(null, self.dataSourceToModel(modelName, response));
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return done(err, null);
            }
        }
    );
};

/**
 * Delete a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {Function} done callback
 */
ESConnector.prototype.destroy = function destroy(modelName, id, done) {
    var self = this;
    if (self.debug) {
        log('destroy', 'model', modelName, 'id', id);
    }

    var filter = self.addDefaults(modelName);
    filter[self.idField] = self.getDocumentId(id);
    if (!filter[self.idField]) {
        throw new Error('Document id not setted!');
    }
    self.db.delete(
        filter
    ).then(
        function (response) {
            done(null, response);
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return done(err, null);
            }
        }
    );
};

/**
 * Update a model instance by id
 *
 * NOTES:
 * > The _source field need to be enabled for this feature to work.
 */
ESConnector.prototype.updateAttributes = function updateAttrs(modelName, id, data, callback) {
    var self = this;
    log('ESConnector.prototype.updateAttributes', 'modelName', modelName, 'id', id, 'data', data);

    if (id===undefined || id===null) {
        throw new Error('id not set!');
    }

    var defaults = self.addDefaults(modelName);
    self.db.update(_.defaults({
        id: id,
        body: {
            doc : data,
            'doc_as_upsert': false
        }
    }, defaults)).then(
        function (response) {
            //console.log('response:',response);
            // TODO: what does the framework want us to return as arguments w/ callback?
            callback(null, response);
            //callback(null, response._id);
            //callback(null, data);
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return callback(err, null);
            }
        }
    );
};

/**
 * Check existence of a model instance by id
 * @param {String} model name
 * @param {String} id row identifier
 * @param {function} done callback
 */
ESConnector.prototype.exists = function (modelName, id, done) {
    var self = this;
    log('ESConnector.prototype.exists', 'model', modelName, 'id', id);

    if (id===undefined || id===null) {
        throw new Error('id not set!');
    }

    var defaults = self.addDefaults(modelName);
    self.db.exists(_.defaults({
        id: self.getDocumentId(id)
    }, defaults)).then(
        function (exists) {
            done(null, exists);
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return done(err, null);
            }
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
ESConnector.prototype.save = function (model, data, done) {
    var self = this;
    if (self.debug) {
        log('ESConnector.prototype.save ', 'model', model, 'data', data);
    }

	var idName = self.idName(model);
    var defaults = self.addDefaults(model);
	var id = self.getDocumentId(data[idName]);

	if (id === undefined || id === null) {
		return done('Document id not setted!', null);
	}

    self.db.update( _.defaults({
        id: id,
        body : {
            doc: data,
			'doc_as_upsert': false
        }
    }, defaults))
        .then(function (response) {
            done(null, response);
        }, function (err) {
            if (err) {
                return done(err, null);
            }
        }
    );
};

/**
 * Find or create a model instance
 */
//ESConnector.prototype.findOrCreate = function (model, data, callback) {...};

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
ESConnector.prototype.updateOrCreate = function updateOrCreate(modelName, data, callback) {
    var self = this;
    log('ESConnector.prototype.updateOrCreate', 'modelName', modelName, 'data', data);

    var idName = self.idName(modelName);
    var id = self.getDocumentId(data[idName]);
    if (id===undefined || id===null) {
        throw new Error('id not set!');
    }

    var defaults = self.addDefaults(modelName);
    self.db.update(_.defaults({
        id: id,
        body: {
            doc : data,
            'doc_as_upsert': true
        }
    }, defaults)).then(
        function (response) {
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
             *          - Most connectors don't fetch the inserted/updated data and hope the data stored into DB
             *            will be the same as the data sent to DB for create/update.
             *          - It's true in most cases but not always. For example, the DB might have triggers
             *            that change the value after the insert/update.
             *            - We don't support that yet.
             *            - In the future, that can be controlled via an options property,
             *              such as fetchNewInstance = true.
             *
             * NOTE: Q1 based approach has been implemented for now.
             */
            if (response._version === 1) { // distinguish if it was an update or create operation in ES
                data[idName] = response._id;
                log('ESConnector.prototype.updateOrCreate', 'assigned ID', idName, '=', response._id);
            }
            callback(null, data, {isNewInstance:response.created});
        }, function (err) {
            console.trace(err.message);
            if (err) {
                return callback(err, null);
            }
        }
    );
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
 * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
 * @param {Function} [cb] The callback function
 */
ESConnector.prototype.automigrate = require('./automigrate.js')({
  log: log
  ,lodash: _
  ,bluebird: Promise
});

module.exports.name = ESConnector.name;
module.exports.ESConnector = ESConnector;
