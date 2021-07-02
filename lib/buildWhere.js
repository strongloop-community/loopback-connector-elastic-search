const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function buildWhere(model, idName, where) {
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
            'docType.keyword': model
          }
        }]
      }
    };
  }
  return body;
}

module.exports.buildWhere = buildWhere;
