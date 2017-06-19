'use strict';

const
  fs = require('fs'),
  pkg = require('../package.json'),
  business = require('../index.js'),
  testData = require('./dataset/in/test.json'),
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
let checkAndDeleteIndex = function(cbCheck) {
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


    it('insertion ou intégration de la notice 1', function (done) {
      let docObject;
      business.doTheJob(docObject = testData[0], function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(99);
        esClient.search({
          index: esConf.index
        }, function (esError, response) {
          expect(esError).to.be.undefined;
          expect(response.hits.total).to.be.equal(1);
          done();
        });
      });
    });

    it('insertion ou intégration de la notice 2', function (done) {
      let docObject;
      business.doTheJob(docObject = testData[1], function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(1);
        esClient.search({
          index: esConf.index
        }, function (esError, response) {
          expect(esError).to.be.undefined;
          expect(response.hits.total).to.be.equal(1);
          expect(response.hits.hits[0]._source.source[1].name).to.be.equal("TU2");
          done();
        });
      });
    });

    it('insertion ou intégration de la notice 3', function (done) {
      let docObject;
      business.doTheJob(docObject = testData[2], function (err) {
        expect(err).to.be.undefined;
        expect(docObject.conditor_ident).to.be.equal(2);
        esClient.search({
          index: esConf.index
        }, function (esError, response) {
          expect(esError).to.be.undefined;
          expect(response.hits.total).to.be.equal(1);
          expect(response.hits.hits[0]._source.source[2].name).to.be.equal("TU3");
          done();
        });
      });
    });


  });

  // Méthde finale sensée faire du nettoyage après les tests
  after(function (done) {
    esClient.indices.delete({index: esConf.index}).then(
      function () {
        done();
      });
    done();
  });


});
