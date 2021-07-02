/*global getSettings getDataSource expect*/
/*eslint no-console: ["error", { allow: ["trace","log"] }] */
describe('Connector', function() {
  var testConnector;

  before(function() {
    require('./init.js');
    var settings = getSettings();
    settings.log = 'error';
    var datasource = getDataSource(settings);
    testConnector = datasource.connector;

    datasource.define('MockLoopbackModel', {
      // here we want to let elasticsearch auto-populate a field that will be mapped back to loopback as the `id`
      id: { type: String, generated: true, id: true }
    });
  });

  it('should configure defaults when building filters', function(done) {
    var modelName = 'MockLoopbackModel';
    var defaults = testConnector.addDefaults(modelName);

    expect(defaults.index).to.be.a('string').to.have.length.above(1).to.match(/^[a-z0-9.-_]+$/i);
    expect(defaults.type).to.be.a('string').to.have.length.above(1).to.match(/^[a-z0-9.-_]+$/i);

    done();
  });

  it('should build a query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      'where': {
        'title': 'Futuro'
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index')
      .that.is.a('string');
    expect(filterCriteria).to.have.property('type')
      .that.is.a('string')
      .that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: { // c. here we are testing the bigger picture `should build a query for the WHERE filter`
          bool: {
            must: [{
              match: {
                title: 'Futuro'
              }
            }, {
              match: {
                'doctype.keyword': modelName
              }
            }]
          }

        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should use a NATIVE filter query as-is', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      'native': {
        query: {
          bool: {
            must: [{
              match: {
                title: 'Futuro'
              }
            }]
          }
        }
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index')
      .that.is.a('string');
    expect(filterCriteria).to.have.property('type')
      .that.is.a('string')
      .that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({
        query: {
          bool: {
            must: [{
              match: {
                title: 'Futuro'
              }
            }]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a simple "and" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        and: [
          { name: 'John Lennon' },
          { role: 'lead' }
        ]
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                match: {
                  name: 'John Lennon'
                }
              },
              {
                match: {
                  role: 'lead'
                }
              },
              {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }

        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a complex "and"  query with "inq" for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        and: [
          { id: { inq: [0, 1, 2] } },
          { vip: true }
        ]
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                terms: {
                  _id: [
                    0,
                    1,
                    2
                  ]
                }
              },
              {
                match: {
                  vip: true
                }
              },
              {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a nested "or" and "and" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        or: [
          { and: [{ id: { inq: [3, 4, 5] } }, { vip: true }] },
          { role: 'lead' }
        ]
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
              match: {
                'doctype.keyword': modelName
              }
            }, {
              bool: {
                should: [{
                    bool: {
                      must: [{
                          terms: {
                            _id: [
                              3,
                              4,
                              5
                            ]
                          }
                        },
                        {
                          match: {
                            vip: true
                          }
                        }
                      ]
                    }
                  },
                  {
                    match: {
                      role: 'lead'
                    }
                  }
                ]
              }
            }]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a nested "and" and "or" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        and: [
          { or: [{ id: { inq: [3, 4, 5] } }, { vip: true }] },
          { role: 'lead' }
        ]
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                bool: {
                  should: [{
                      terms: {
                        _id: [
                          3,
                          4,
                          5
                        ]
                      }
                    },
                    {
                      match: {
                        vip: true
                      }
                    }
                  ]
                }
              },
              {
                match: {
                  role: 'lead'
                }
              },
              {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a "nin" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      'where': {
        'id': {
          'nin': [0, 1, 2]
        }
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
              match: {
                'doctype.keyword': modelName
              }
            }, {
              bool: {
                must_not: [{
                  terms: {
                    _id: [
                      0,
                      1,
                      2
                    ]
                  }
                }]
              }
            }]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a "and" and "nin" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        and: [
          { id: { nin: [0, 1, 2] } },
          { vip: true }
        ]
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                bool: {
                  must_not: [{
                    terms: {
                      _id: [
                        0,
                        1,
                        2
                      ]
                    }
                  }]
                }
              },
              {
                match: {
                  vip: true
                }
              },
              {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a "between" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        order: {
          between: [3, 6]
        }
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
              range: {
                order: {
                  gte: 3,
                  lte: 6
                }
              }
            }, {
              match: {
                'doctype.keyword': modelName
              }
            }]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build a "and" and "between" query for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        and: [
          { order: { between: [2, 6] } },
          { vip: true }
        ]
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                range: {
                  order: {
                    gte: 2,
                    lte: 6
                  }
                }
              },
              {
                match: {
                  vip: true
                }
              }, {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build multiple normal where clause query without "and" for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        role: 'lead',
        vip: true
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                match: {
                  role: 'lead'
                }
              },
              {
                match: {
                  vip: true
                }
              }, {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });

  it('should build two "inq" and one "between" without "and" for the WHERE filter', function(done) {
    var criteria, size, offset, modelName, modelIdName, settings, mappingType;
    criteria = {
      where: {
        role: { inq: ['lead'] },
        order: { between: [1, 6] },
        id: { inq: [2, 3, 4, 5] }
      }
    };
    size = 100;
    offset = 10;
    modelName = 'MockLoopbackModel';
    modelIdName = 'id';
    settings = getSettings();
    mappingType = settings.mappingType;

    var filterCriteria = testConnector.buildFilter(modelName, modelIdName, criteria, size, offset);
    expect(filterCriteria).not.to.be.null;
    expect(filterCriteria).to.have.property('index').that.is.a('string');
    expect(filterCriteria).to.have.property('type').that.is.a('string').that.equals(mappingType);
    expect(filterCriteria).to.have.property('body')
      .that.is.an('object')
      .that.deep.equals({ // a. this is really 2 tests in one
        sort: [
          // b. `_id` is an auto-generated field that ElasticSearch populates for us
          // when we want to let the backend/system/ES take care of id population
          // so if we want to sort by id, without specifying/controlling our own id field,
          // then ofcourse the sort must happen on `_id`, this part of the test, validates that!
          '_id'
        ],
        query: {
          bool: {
            must: [{
                terms: {
                  role: [
                    'lead'
                  ]
                }
              },
              {
                range: {
                  order: {
                    gte: 1,
                    lte: 6
                  }
                }
              },
              {
                terms: {
                  _id: [
                    2,
                    3,
                    4,
                    5
                  ]
                }
              }, {
                match: {
                  'doctype.keyword': modelName
                }
              }
            ]
          }
        }
      });
    expect(filterCriteria).to.have.property('size')
      .that.is.a('number');
    expect(filterCriteria).to.have.property('from')
      .that.is.a('number');

    done();
  });
});