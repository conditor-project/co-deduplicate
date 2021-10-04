/* eslint-env mocha */
/* eslint-disable no-unused-expressions */
'use strict';

const rewire = require('rewire');
const pkg = require('../package.json');
const business = rewire('../index.js');
const testData = require('./dataset/in/test.json');
const baseRequest = require('co-config/base_request.json');
const chai = require('chai');
const debug = require('debug')('test');
const expect = chai.expect;
const _ = require('lodash');
const es = require('elasticsearch');

var esConf = require('co-config/es.js');

business.__set__('esConf.index', esConf.index);
business.__set__('esConf.httpAuth', esConf.httpAuth);
business.__set__('esConf.host', esConf.host);

const esClient = new es.Client({
  host: esConf.host,
  httpAuth: esConf.httpAuth,
  log: {
    type: 'file',
    level: ['error']
  }
});

// fonction de vérification et suppression de l'index pour les tests
let checkAndDeleteIndex = function (cbCheck) {
  esClient.indices.exists({ index: esConf.index }, function (errorExists, exists) {
    if (errorExists) {
      console.error(`Problème dans la vérification de l'index ${esConf.index}\n${errorExists.message}`);
      process.exit(1);
    }
    if (!exists) { return cbCheck(); }
    esClient.indices.delete({ index: esConf.index }, function (errorDelete, responseDelete) {
      if (errorDelete) {
        console.error(`Problème dans la suppression de l'index ${esConf.index}\n${errorDelete.message}`);
        process.exit(1);
      }
      return cbCheck();
    });
  });
};

describe(pkg.name + '/index.js', function () {
  this.timeout(10000);

  // Méthde d'initialisation s'exécutant en tout premier
  before(function (done) {
    checkAndDeleteIndex(function (errCheck) {
      if (errCheck) {
        console.log('Erreur checkAndDelete() : ' + errCheck.errMessage);
        process.exit(1);
      }

      business.beforeAnyJob(function (errBeforeAnyJob) {
        if (errBeforeAnyJob) {
          console.log('Erreur beforeAnyJob(), code ' + errBeforeAnyJob.errCode);
          console.log(errBeforeAnyJob.errMessage);
          process.exit(1);
        }
        console.log('before OK');
        done();
      });
    });
  });

  describe('#fonction loadScripts', function () {
    it('devrait lire les fichiers de script et reconstituer l\'objet scriptList', (done) => {
      const scriptList = business.__get__('loadPainlessScripts')();
      debug(Object.keys(scriptList));
      expect(Object.keys(scriptList).length, 'il devrait y avoir au moins 7 scripts painless dans la liste').to.be.gte(7);
      const expectedScripts = ['addDuplicate', 'addEmptyDuplicate', 'removeDuplicate', 'setDuplicateRules', 'setHasTransDuplicate', 'setSourceUidChain', 'setIsDuplicate'];
      expect(_.intersection(expectedScripts, Object.keys(scriptList)).length, 'Les au moins 7 scripts painless doivent avoir le bon nom').to.be.gte(7);
      done();
    });
  });

  // test sur la création de règle
  describe('#fonction buildQuery', function () {
    let docObject;
    let request = _.cloneDeep(baseRequest);

    it('Le constructeur de requête devrait pour la notice remonter 15 règles', function (done) {
      docObject = testData[0];
      request = business.__get__('buildQuery')(docObject, request);
      const arxivQuery = _.find(request.query.bool.should, (clause) => {
        return clause.bool._name.indexOf(' : 1ID:arxiv+doi') > 0;
      });

      expect(arxivQuery.bool.must[0].bool.should[0].match).to.have.key('arxiv.normalized');
      expect(request.query.bool.should.length).to.be.gte(15);
      done();
    });
  });
  // test sur l'insertion d'une 1ere notice
  describe('#insert notice 1', function () {
    let totalExpected = 0;
    testData.map((data, index) => {
      it(data._comment, function (done) {
        business.doTheJob(data, function (err) {
          if (err) return done(err.errMessage);
          esClient.search({
            index: esConf.index
          }, function (esError, response) {
            if (esError) return done(esError);
            if (index !== 7) totalExpected++; // erreur normale sur le 7ème doc
            expect(response.hits.total.value).to.be.equal(totalExpected);
            expect(response.hits.hits[0]._source.sourceUidChain).not.to.be.undefined;
            response.hits.hits.forEach(hit => {
              const doc = hit._source;
              if (doc.sourceUid === 'crossref$10.1021/jz502360c') {
                expect(doc.isDuplicate,"isDuplicate doit valoir true").to.be.equal(true);
                expect(doc.duplicates.length, "le tableau duplicates doit contenir au moins un élément").to.be.gte(1);
                expect(doc.duplicates.length).to.be.gte(1);
                expect(doc.duplicates[0].idConditor==='Qd74UnItx6nGYLwrBc2MDZF8k');
                expect(doc.duplicates[0].rules[0].indexOf('2Collation'),
                "doit matcher avec la règle 2Collation...").to.be.gte(0);
              }
            });
            done();
          });
        });
      });
    });
  });
  /*
  describe('#tests des normalizer', function () {
    it('Titre normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'title.default.normalized',
          'text': 'Voici un test de titre caparaçonner aïoli ! '
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('voiciuntestdetitrecaparaconneraioli');
        done();
      });
    });

    it('Auteur normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'first3AuthorNames.normalized',
          'text': 'Gérard Philippe, André Gide'
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('gerardphilippeandregide');
        done();
      });
    });

    it('ID normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'doi.normalized',
          'text': '1586-544984Efrea'
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('1586544984efrea');
        done();
      });
    });

    it('Page normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'pageRange.normalized',
          'text': '158-165'
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('158');
        done();
      });
    });

    it('Volume normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'volume.normalized',
          'text': 'v52'
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('52');
        done();
      });
    });
    it('Numero normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'issue.normalized',
          'text': 'V14'
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('14');
        done();
      });
    });
    it('publicationDate normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: esConf.index,
        body: {
          'field': 'publicationDate.normalized',
          'text': '18-11-2012'
        }
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('2012');
        done();
      });
    });
  });*/
  
  after(function () {
    return esClient.indices.delete({ index: esConf.index });
  });
});
