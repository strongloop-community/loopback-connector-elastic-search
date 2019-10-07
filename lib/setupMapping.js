let _ = null;
let log = null;

const setupMapping = () => {
  const self = this;
  const {
    db,
    settings
  } = self;
  const mappingType = self.settings.mappingType || 'basedata';
  const mappingsFromDatasource = [{
    name: mappingType,
    properties: settings.mappingProperties
  }];
  log('ESConnector.prototype.setupMapping', 'mappingsFromDatasource:', mappingsFromDatasource);

  if (mappingsFromDatasource.length === 0) {
    log('ESConnector.prototype.setupMapping', 'missing mapping for mappingType:', mappingType,
      ' ... this usecase is legitimate if you want elasticsearch to take care of mapping dynamically');
    return Promise.resolve();
  }
  if (mappingsFromDatasource.length > 1) {
    return Promise.reject(new Error('more than one mapping for mappingType:', mappingType));
  }
  log('ESConnector.prototype.setupMapping', 'found mapping for mappingType:', mappingType);
  // NOTE: this is where the magic happens (below line)
  const defaults = self.addDefaults(mappingsFromDatasource[0].name);
  const mapping = _.clone(mappingsFromDatasource[0]);

  // TODO: create a method called cleanUpMapping or something like that to blackbox this stuff
  delete mapping.name;
  // delete mapping.index;
  delete mapping.type;

  // adding 'docType' mandatory keyword field to mapping properties
  mapping.properties.docType = {
    type: 'keyword',
    index: true
  };

  log('ESConnector.prototype.setupMapping', 'will setup mapping for mappingType:', mappingsFromDatasource[0].name);

  // return self.setupIndices(defaults.index)
  return self.setupIndex(defaults.index).then(() => {
    log('ESConnector.prototype.setupMapping', 'db.indices.putMapping', 'mappingType:', mappingType, 'start');
    return db.indices.putMapping(_.defaults({
      body: mapping
    }, defaults)).then((body) => {
      log('ESConnector.prototype.setupMapping', 'db.indices.putMapping', 'mappingType:', mappingType, 'response', body);
      return Promise.resolve();
    }, (err) => {
      log('ESConnector.prototype.setupMapping', 'db.indices.putMapping', 'mappingType:', mappingType, 'failed', err);
      // console.trace(err.message);
      return Promise.reject(err);
    });
  });
};

module.exports = (dependencies) => {
  log = dependencies
    // eslint-disable-next-line no-console
    ? (dependencies.log || console.log)
    // eslint-disable-next-line no-console
    : console.log;
  _ = (dependencies) ? (dependencies.lodash || require('lodash')) : require('lodash');
  return setupMapping;
};
