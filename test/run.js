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
      console.err(`Problème dans la vérification de l'index ${esConf.index}\n${errorExists.message}`);
      process.exit(1);
    }
    if (!exists) return cbCheck();
    esClient.indices.delete({index: esConf.index}, function (errorDelete, responseDelete) {
      if (errorDelete) {
        console.err(`Problème dans la suppression de l'index ${esConf.index}\n${errorDelete.message}`);
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
        if (err) {
          console.log(err.errCode);
          console.log(err.errMessage);
          //process.exit(1);
        }
        console.log('post-doTheJob-doc1');
        done();
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
