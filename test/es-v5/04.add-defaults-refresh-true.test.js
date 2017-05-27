/*eslint no-console: "off"*/
/*global getSchema should assert*/
describe('Add Defaults', function () {
    var testConnector, testConnector2, db, db2;

    before(function (done) {
        require('./init.js');
        var settings = getSettings();
        settings.log = 'error';
        db = getDataSource(settings);

        var settings2 = getSettings();
        settings2.refreshOn = ["save", "updateAttributes"];
        db2 = getDataSource(settings2);

        var account = {real_name: {type: String, index: true, sort: true}};
        db2.define("Account", account);
        db.define("Account", account);
        var bookProps = {real_name: {type: String, index: true, sort: true}};
        var bookSettings = {
            "properties": {
                "real_name": {
                    "type": "keyword"
                }
            },
            "elasticsearch": {
                "create": {
                    "refresh": false
                },
                "destroy": {
                    "refresh": false
                },
                "destroyAll": {
                    "refresh": "wait_for"
                }
            }
        };
        db.define("Book", bookProps, bookSettings);
        db2.define("Book", bookProps, bookSettings);
        testConnector = db.connector;
        testConnector2 = db2.connector;
        db.automigrate(done);
    });

    describe('Datasource specific settings', function () {

        it('modifying operations should have refresh true', function () {
            (typeof testConnector2.addDefaults('Account', 'create').refresh === 'undefined').should.be.true;
            (testConnector2.addDefaults('Account', 'save').refresh === true).should.be.true;
            (typeof testConnector2.addDefaults('Account', 'destroy').refresh === 'undefined').should.be.true;
            (typeof testConnector2.addDefaults('Account', 'destroyAll').refresh === 'undefined').should.be.true;
            (testConnector2.addDefaults('Account', 'updateAttributes').refresh === true).should.be.true;
            (typeof testConnector2.addDefaults('Account', 'updateOrCreate').refresh === 'undefined').should.be.true;
        });

        it('create and destroy should have refresh false for model book', function () {
            (testConnector2.addDefaults('Book', 'destroy').refresh === false).should.be.true;
            (testConnector2.addDefaults('Book', 'create').refresh === false).should.be.true;
            (testConnector2.addDefaults('Book', 'save').refresh === true).should.be.true;
            (testConnector2.addDefaults('Book', 'destroyAll').refresh === 'wait_for').should.be.true;
            (testConnector2.addDefaults('Book', 'updateAttributes').refresh === true).should.be.true;
            (typeof testConnector2.addDefaults('Book', 'updateOrCreate').refresh === 'undefined').should.be.true;
        });

        it('should never have a refresh attribute', function () {
            (typeof testConnector.addDefaults('Book', 'removeMappings').refresh === 'undefined').should.be.true;
            (typeof testConnector.addDefaults('Book', 'buildFilter').refresh === 'undefined').should.be.true;
            (typeof testConnector.addDefaults('Book', 'find').refresh === 'undefined').should.be.true;
            (typeof testConnector.addDefaults('Book', 'exists').refresh === 'undefined').should.be.true;
            (typeof testConnector.addDefaults('Book', 'count').refresh === 'undefined').should.be.true;
        });
    });

    describe('Model specific settings', function () {

        it('modifying operations should have refresh true', function () {
            (testConnector.addDefaults('Account', 'create').refresh === true).should.be.true;
            (testConnector.addDefaults('Account', 'save').refresh === true).should.be.true;
            (testConnector.addDefaults('Account', 'destroy').refresh === true).should.be.true;
            (testConnector.addDefaults('Account', 'destroyAll').refresh === true).should.be.true;
            (testConnector.addDefaults('Account', 'updateAttributes').refresh === true).should.be.true;
            (testConnector.addDefaults('Account', 'updateOrCreate').refresh === true).should.be.true;

        });

        it('create and destroy should have refresh false for model book', function () {
            (testConnector.addDefaults('Book', 'destroy').refresh === false).should.be.true;
            (testConnector.addDefaults('Book', 'create').refresh === false).should.be.true;
            (testConnector.addDefaults('Book', 'save').refresh === true).should.be.true;
            (testConnector.addDefaults('Book', 'destroyAll').refresh === 'wait_for').should.be.true;
            (testConnector.addDefaults('Book', 'updateAttributes').refresh === true).should.be.true;
            (testConnector.addDefaults('Book', 'updateOrCreate').refresh === true).should.be.true;
        });

    });
});