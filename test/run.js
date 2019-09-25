/* eslint-env mocha */
/* eslint-disable no-unused-expressions */
'use strict';

const rewire = require('rewire');
const Promise = require('bluebird');
const pkg = require('../package.json');
const coDeduplicate = rewire('../index.js');
const testData = require('./dataset/in/test.json');
const baseRequest = require('co-config/base_request.json');
const chai = require('chai');
const debug = require('debug')('test');
const expect = chai.expect;
const _ = require('lodash');
const es = require('elasticsearch');

var esConf = require('co-config/es.js');
const esMapping = require('co-config/mapping.json');
esConf.index = `tests-deduplicate-${Date.now()}`;
coDeduplicate.__set__('esConf.index', esConf.index);

const esClient = new es.Client({
  host: esConf.host,
  log: {
    type: 'file',
    level: ['error']
  }
});

describe(pkg.name + '/index.js', function () {
  this.timeout(0);

  before(function (done) {
    const docs = [];
    const options = {
      index: {
        _index: esConf.index,
        _type: esConf.type
      }
    };
    testData.map(data => {
      docs.push(options);
      docs.push(data);
    });
    esClient.indices.create({ index: esConf.index, body: esMapping })
      .then(() => esClient.bulk({ body: docs }))
      .then(() => Promise.delay(2000))
      .then(() => done())
      .catch(error => done(error));
  });

  describe('#fonction loadScripts', function () {
    it('devrait lire les fichiers de script et reconstituer l\'objet scriptList', (done) => {
      const scriptList = coDeduplicate.__get__('loadPainlessScripts')();
      debug(Object.keys(scriptList));
      expect(Object.keys(scriptList).length, 'il devrait y avoir au moins 7 scripts painless dans la liste').to.be.gte(7);
      const expectedScripts = ['addDuplicate', 'addEmptyDuplicate', 'removeDuplicate', 'setDuplicateRules', 'setHasTransDuplicate', 'setIdChain', 'setIsDuplicate'];
      expect(_.intersection(expectedScripts, Object.keys(scriptList)).length, 'Les au moins 7 scripts painless doivent avoir le bon nom').to.be.gte(7);
      done();
    });
  });

  describe('#fonction buildQuery', function () {
    let docObject;
    let request = _.cloneDeep(baseRequest);

    it('Le constructeur de requête devrait pour la notice remonter 15 règles', function (done) {
      docObject = testData[0];
      request = coDeduplicate.__get__('buildQuery')(docObject, request);
      const arxivQuery = _.find(request.query.bool.should, (clause) => {
        return clause.bool._name.indexOf(' : 1ID:arxiv+doi') > 0;
      });
      expect(arxivQuery.bool.must[0].bool.should[0].match).to.have.key('arxiv.normalized');
      expect(request.query.bool.should.length).to.be.gte(15);
      done();
    });
  });

  describe('#doTheJob', function () {
    testData.map((doc, index) => {
      it(`Notice ${index + 1}`, function (done) {
        coDeduplicate.doTheJob(doc, function (err) {
          if (err) return done(err);
          expect(doc.isDuplicate).to.be.true;
          expect(doc.duplicates).to.be.an('Array');
          expect(doc.duplicates.length).to.be.gte(1);
          doc.duplicates.map(duplicate => {
            expect(duplicate.rules).to.be.an('Array');
            expect(duplicate.rules.length).to.be.gte(1);
          });
          if (doc.sourceUid === 'crossref$10.1021/jz502360c') {
            expect(doc.duplicates[0].idConditor === 'Qd74UnItx6nGYLwrBc2MDZF8k');
            expect(doc.duplicates[0].rules[0].indexOf('2Collation'), 'doit matcher avec la règle 2Collation...').to.be.gte(0);
          }
          done();
        });
      });
    });

    it('should update data in elasticsearch', function () {
      return Promise.delay(2000)
        .then(() => esClient.search({ index: esConf.index, size: 20 }))
        .then(response => {
          response.hits.hits.forEach(hit => {
            const doc = hit._source;
            expect(doc.isDuplicate).to.be.true;
            expect(doc.duplicates).to.be.an('Array');
            expect(doc.duplicates.length).to.be.gte(1);
            if (doc.sourceUid === 'crossref$10.1021/jz502360c') {
              expect(doc.duplicates[0].idConditor === 'Qd74UnItx6nGYLwrBc2MDZF8k');
              expect(doc.duplicates[0].rules[0].indexOf('2Collation'), 'doit matcher avec la règle 2Collation...').to.be.gte(0);
            }
          });
        })
      ;
    });
  });

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
  });

  after(function () {
    return esClient.indices.delete({ index: esConf.index });
  });
});
