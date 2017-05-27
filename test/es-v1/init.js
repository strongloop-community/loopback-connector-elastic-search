'use strict';

/**
 * Why does this file exist?
 *
 * Individual tests can load the datasource, and avoid repetition, by adding:
 *   `require('./init.js');`
 * of their source code.
 */

var chai = require('chai');
global.expect = chai.expect;
global.assert = chai.assert;
global.should = chai.should(); // Why is the function being executed? Because the "should" interface extends Object.prototype to provide a single getter as the starting point for your language assertions.

global._ = require('lodash'); /*global _:true*/

var settings = require('./datasource-test-v1-plain.json');
global.getSettings = function() { /*global getSettings*/
    return _.cloneDeep(settings);
};

var DataSource = require('loopback-datasource-juggler').DataSource;
global.getDataSource = global.getSchema = global.getConnector = function (customSettings) {
    (customSettings) /*eslint no-console: ["error", { allow: ["log"] }] */
        ? console.log('\n\tcustomSettings will override global settings for datasource\n'/*, JSON.stringify(customSettings,null,2)*/)
        : console.log('\n\twill use global settings for datasource\n');
    var settings = customSettings || getSettings();
    // settings.log = {
    //     type: 'file',
    //     level: 'trace',
    //     path: 'test/es-v1/elasticsearch-v1-'+Date.now()+'.log'
    // };
    //console.log('\n\tsettings:\n', JSON.stringify(settings,null,2));
    settings.connector =  require('../../');
    return new DataSource(settings);
};
