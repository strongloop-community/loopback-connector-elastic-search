const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function buildDeepNestedQueries(
  root,
  idName,
  where,
  body,
  queryPath,
  model,
  nestedFields
) {
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
}

module.exports.buildDeepNestedQueries = buildDeepNestedQueries;
