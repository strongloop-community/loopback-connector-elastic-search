
/*eslint no-console: "off"*/
/*global getSchema should assert*/
describe('Add Defaults', function () {
    var testConnector, db, Account, Book;

    before(function (done) {
        require('./init.js');
        var settings = getSettings();
        settings.log = 'error';
        db = getDataSource(settings);
        Account = db.define("Account", {
            real_name: {type: String, index: true, sort: true}
        });
        Book = db.define("Book", {
            real_name: {type: String, index: true, sort: true}
        }, {
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
        });
        testConnector = db.connector;
        db.automigrate(done);
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

    describe('Per call specific', function (done) {
        it('create refresh false', function () {
            Account.create({real_name: "test123"}, {refresh: false}, function(err, response) {
                should.not.exist(err);
                Account.count(function(result) {
                    (result == 0).should.be.true;
                    done();
                });
            })
        })
        it('create refresh true', function (done) {
            Book.create({real_name: "test123"}, {refresh: true}).then(function() {
                Book.count().then(function(result) {
                    (result == 1).should.be.true;
                    done();
                }, function(error) {
                    should.not.exist(error);
                    done(error);
                }).catch(function(error) {
                    done(error);
                })
            }, function(error) {
                done(error);
            }).catch(function(error) {
                done(error);
            });
        })
    });
});