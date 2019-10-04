module.exports = {
  'db': {
    'name': 'elasticsearch-6',
    'connector': 'esv6',
    'index': 'shakespeare',
    'hosts': [{
      'host': 'localhost',
      'port': 9202
    }],
    'apiVersion': '6.0',
    'log': 'trace',
    'defaultSize': 50,
    'requestTimeout': 30000,
    'mappings': [],
    "mappingType": "basedata",
    "mappingProperties": {
      "id": {
        "type": "keyword",
        "index": true
      },
      "docType": {
        "type": "keyword",
        "index": true
      },
      "name": {
        "type": "text",
        "index": true
      },
      'realm': { 
        'type': 'keyword', 
        'index': true 
      },
      'username': { 
        'type': 'keyword', 
        'index': true 
      },
      "description": {
        "type": "text",
        "index": true
      },
      "roleId": {
        "type": "keyword",
        "index": true
      },
      "principalId": {
        "type": "keyword",
        "index": true
      },
      "principalType": {
        "type": "keyword",
        "index": true
      },
      "model": {
        "type": "keyword",
        "index": true
      },
      "property": {
        "type": "keyword",
        "index": true
      },
      "accessType": {
        "type": "keyword",
        "index": true
      },
      "permission": {
        "type": "keyword",
        "index": true
      },
      "userId": {
        "type": "keyword",
        "index": true
      },
      "ttl": {
        "type": "long",
        "index": true
      },
      "email": {
        "type": "keyword",
        "index": true
      },
      "password": {
        "type": "keyword",
        "index": false
      },
      "status": {
        "type": "keyword",
        "index": true
      },
      "created": {
        "type": "date",
        "index": true
      },
      "modified": {
        "type": "date",
        "index": true
      },
      "lastUpdated": {
        "type": "date",
        "index": true
      }
    }
  }
};
