const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function buildNestedQueries(body, model, idName, where, nestedFields) {
  /**
   * Return an empty match all object if no property is set in where filter
   * @example {where: {}}
   */
  const self = this;
  if (_.keys(where).length === 0) {
    body = {
      query: {
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
      }
    };
    log('ESConnector.prototype.buildNestedQueries', '\nbody', JSON.stringify(body, null, 0));
    return body;
  }
  const rootPath = body.query;
  self.buildDeepNestedQueries(true, idName, where,
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
        'docType.keyword': model
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
}

module.exports.buildNestedQueries = buildNestedQueries;
