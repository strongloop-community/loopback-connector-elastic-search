const log = require('debug')('loopback:connector:elasticsearch');

async function setupIndex() {
  const self = this;
  const {
    db,
    version,
    index,
    settings: {
      mappingProperties,
      mappingType,
      indexSettings
    }
  } = self;
  const { body: exists } = await db.indices.exists({
    index
  });
  mappingProperties.docType = {
    type: 'keyword',
    index: true
  };
  const mapping = {
    properties: mappingProperties
  };
  if (!exists) {
    log('ESConnector.prototype.setupIndex', 'create index with mapping for', index);
    await db.indices.create({
      index,
      body: {
        settings: indexSettings,
        mappings: version < 7 ? {
          [mappingType]: mapping
        } : mapping
      }
    });
    return Promise.resolve();
  }
  const updateMapping = {
    index,
    body: mapping
  };
  log('ESConnector.prototype.setupIndex', 'update mapping for index', index);
  if (version < 7) {
    updateMapping.type = mappingType;
  }
  await db.indices.putMapping(updateMapping);
  return Promise.resolve();
}

module.exports.setupIndex = setupIndex;
