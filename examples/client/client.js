const dataSource = {
  "name": "elasticsearch-example-index-datasource",
  "connector": "esv6",
  "version": 6,
  "index": "example-index",
  "configuration": {
    "node": "http://localhost:9200",
    "requestTimeout": 30000,
    "pingTimeout": 3000
  },
  "defaultSize": 50,
  "indexSettings": {},
  "mappingType": "basedata",
  "mappingProperties": {
    "id": {
      "type": "keyword"
    },
    "seq": {
      "type": "integer"
    },
    "name": {
      "type": "keyword",
      "fields": {
        "native": {
          "type": "keyword"
        }
      }
    },
    "email": {
      "type": "keyword"
    },
    "birthday": {
      "type": "date"
    },
    "role": {
      "type": "keyword"
    },
    "order": {
      "type": "integer"
    },
    "vip": {
      "type": "boolean"
    },
    "objectId": {
      "type": "keyword"
    },
    "ttl": {
      "type": "integer"
    },
    "created": {
      "type": "date"
    }
  }
};

const SupportedVersions = [6, 7]; // Supported elasticsearch versions
// 'Client' will be assigned either Client6 or Client7 from below definitions based on version
let Client = null;
const { Client: Client6 } = require('es6');
const { Client: Client7 } = require('es7');
const version = 6;
Client = version === 6 ? Client6 : Client7;
const db = new Client(dataSource.configuration);

db.ping().then(({ body }) => {
  console.log(body);
}).catch((e) => {
  console.log(e);
});

db.indices.create({
  index: 'helloworld',
  body: {
    settings: {},
    mappings: {
      basedata: {
        properties: {
          name: {
            type: 'keyword'
          }
        }
      }
    }
  }
}).then((response) => {
  console.log(response);
}).catch((e) => {
  console.log(e);
});

/* db.indices.putMapping({
  index: 'hello',
  type: 'basedata',
  body: {
    properties: {
      name: {
        type: 'keyword'
      }
    }
  }
}).then(({ body }) => {
  console.log(body);
}).catch((e) => {
  console.log(e);
}); */

db.count({
  index: 'hello'
}).then(({ body }) => {
  console.log(body);
}).catch((e) => {
  console.log(e);
});

/* db.create({
  index: 'hello',
  type: 'basedata',
  id: 'aasddd',
  body: {
    name: 'hello'
  }
}).then(({ body }) => {
  console.log(body);
}).catch((e) => {
  console.log(e);
}); */

/* db.index({
  index: 'hello',
  type: 'basedata',
  body: {
    name: 'hello2s'
  }
}).then(({ body }) => {
  console.log(body, body._id);
}).catch((e) => {
  console.log(e);
}); */
