'use strict';

const
  fs = require('fs'),
  pkg = require('../package.json'),
  business = require('../index.js'),
  testData = require('./dataset/in/test.json'),
  badData = require('./dataset/in/badDocs.json'),
  chai = require('chai'),
  expect = chai.expect,
  es = require('elasticsearch');

var esConf = require('../es.js');
esConf.index = 'tests-undoubler';

const esClient = new es.Client({
  host: esConf.host,
  log: {
    type: 'file',
    level: 'trace'
  }
});


//fonction de vérification et suppression de l'index pour les tests
let checkAndDeleteIndex = function (cbCheck) {
  esClient.indices.exists({index: esConf.index}, function (errorExists, exists) {
    if (errorExists) {
      console.error(`Problème dans la vérification de l'index ${esConf.index}\n${errorExists.message}`);
      process.exit(1);
    }
    if (!exists) return cbCheck();
    esClient.indices.delete({index: esConf.index}, function (errorDelete, responseDelete) {
      if (errorDelete) {
        console.error(`Problème dans la suppression de l'index ${esConf.index}\n${errorDelete.message}`);
        process.exit(1);
      }
      return cbCheck;
    });
  });
};


describe(pkg.name + '/index.js', function () {

  this.timeout(10000);

  // Méthde d'initialisation s'exécutant en tout premier
  before(function (done) {

    checkAndDeleteIndex(function (errCheck) {

      if (errCheck) {
        console.log("Erreur checkAndDelete() : " + err.errMessage);
        process.exit(1);
      }

      business.beforeAnyJob(function (errBeforeAnyJob) {
        if (errBeforeAnyJob) {
          console.log("Erreur beforeAnyJob(), code " + errBeforeAnyJob.errCode);
          console.log(errBeforeAnyJob.errMessage);
          process.exit(1);
        }
        console.log("before OK");
        done();
      });

    });

  });

  // test sur l'insertion d'une 1ere notice
  describe('#insert notice 1', function () {

    let docObject;

    it('La notice 1 devrait être intégrée et seule dans l\'index ES - regle 99', function (done) {
      docObject = testData[0];
      business.doTheJob(docObject = testData[0], function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(99);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(1);
            done();
          });
        }, 300);
      });
    });

    it('La notice 2 devrait matcher sur titre+DOI - regle 1', function (done) {
      docObject = testData[1];
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(1);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(1);
            expect(response.hits.hits[0]._source.source[1].name).to.be.equal("TU2");
            done();
          });
        }, 300);
      });
    });

    it('La notice 3 devrait matcher sur titre+volume+numero+issn - regle 2', function (done) {
      docObject = testData[2];
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(2);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(1);
            expect(response.hits.hits[0]._source.source[2].name).to.be.equal("TU3");
            done();
          });
        }, 300);
      });
    });


    it('La notice 4 devrait matcher sur DOI seul - regle 3', function (done) {
      docObject = testData[3];
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(3);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(1);
            expect(response.hits.hits[0]._source.source[3].name).to.be.equal("TU4");
            done();
          });
        }, 300);
      });
    });

    it('La notice 5 devrait matcher sur titre+auteur+issn - regle 4', function (done) {
      docObject = testData[4];
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(4);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(1);
            expect(response.hits.hits[0]._source.source[4].name).to.be.equal("TU5");
            done();
          });
        }, 300);
      });
    });
    it('La notice 6 devrait match sur titre+auteur_init+issn - regle 5', function (done) {
      docObject = testData[5];
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(5);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(1);
            expect(response.hits.hits[0]._source.source[5].name).to.be.equal("TU6");
            done();
          });
        }, 300);
      });
    });


    it('La notice 7 devrait être reconnue comme un vrai doublon', function (done) {
      docObject = testData[6];
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(99);
        setTimeout(function() {
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            expect(esError).to.be.undefined;
            expect(response.hits.total).to.be.equal(2);
            expect(response.hits.hits[1]._source.source[0].name).to.be.equal("TU7");
            done();
          });
        }, 300);
      });
    });


    it('Les notice 3-1 et 3-2 ne devraient pas matcher entre elles - regle 3bis', function (done) {
      docObject = badData.R3.emptyDOI1;
      business.doTheJob(docObject, function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(99);
        setTimeout(function() {
          let doc2 = badData.R3.emptyDOI2;
          business.doTheJob(doc2, function (err2) {
            expect(err2).to.be.undefined;
            expect(doc2.conditor_ident).to.be.equal(99);
            setTimeout(function() {
              done();
            },300);
          });
        },300);
      });
    });

  });

// Méthde finale sensée faire du nettoyage après les tests
  after(function (done) {
    esClient.indices.delete({index: esConf.index}).then(
      function () {
        console.log("nettoyage index de test OK");
        done();
      });
    done();
  });


});
