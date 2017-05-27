module.exports = {
  'extends': 'eslint:recommended'
  ,'rules': {
    'indent': ['error', 2]
  }
  ,'env': {
    'mocha': true
    ,'node': true
  },
  'globals': {
    'expect': true,
    'getDataSource': true,
    'should': true,
    'assert': true,
    'getSchema': true,
    'getSettings': true
  }
};