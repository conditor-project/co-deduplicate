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
// const debug = require('debug')('test');
const expect = chai.expect;
const _ = require('lodash');
const es = require('elasticsearch');
const generateFakeDoc = require('./dataset/generate-fake-doc.js');

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
  this.timeout(5000);

  before(function () {
    return esClient.indices.create({ index: esConf.index, body: esMapping });
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
    it('shouldn\'t find any duplicate', function () {
      const docOne = generateFakeDoc();
      const docTwo = generateFakeDoc();
      return esClient.bulk({
        body: [
          { index: { _index: esConf.index, _type: esConf.type, _id: docOne.idConditor } },
          docOne,
          { index: { _index: esConf.index, _type: esConf.type, _id: docTwo.idConditor } },
          docTwo
        ],
        refresh: true
      }).then(() => {
        return new Promise((resolve, reject) => {
          coDeduplicate.doTheJob(docOne, function (err) {
            if (err) return reject(err);
            expect(docOne.isDeduplicable).to.be.true;
            expect(docOne.isDuplicate).to.be.false;
            expect(docOne.duplicates).to.be.an('Array').that.is.empty;
            expect(docOne.duplicateRules).to.be.an('Array').that.is.empty;
            expect(docOne.idChain).to.be.a('string');
            expect(docOne.idChain).to.equal(`${docOne.source}:${docOne.idConditor}!`);
            resolve();
          });
        });
      });
    });

    it('should find a duplicate with same doi and title.default (rule 0)', function () {
      const docOne = generateFakeDoc();
      const docTwo = generateFakeDoc();
      docTwo.doi = docOne.doi;
      docTwo.title.default = docOne.title.default;
      return esClient.bulk({
        body: [
          { index: { _index: esConf.index, _type: esConf.type, _id: docOne.idConditor } },
          docOne,
          { index: { _index: esConf.index, _type: esConf.type, _id: docTwo.idConditor } },
          docTwo
        ],
        refresh: true
      }).then(() => {
        return new Promise((resolve, reject) => {
          coDeduplicate.doTheJob(docOne, function (err) {
            if (err) return reject(err);
            expect(docOne.isDuplicate).to.be.true;
            expect(docOne.duplicates).to.be.an('Array');
            expect(docOne.duplicates).to.have.lengthOf(1);
            const duplicate = docOne.duplicates[0];
            expect(duplicate.source).to.be.equal(docTwo.source);
            expect(duplicate.idConditor).to.be.equal(docTwo.idConditor);
            expect(duplicate.rules).to.be.an('Array');
            expect(duplicate.rules.length).to.be.gte(1);
            expect(duplicate.rules).to.include('Article : 1ID:doi+TiC');
            resolve();
          });
        });
      });
    });

    it('should find a duplicate with same pmId and title.default (rule 2)', function () {
      const docOne = generateFakeDoc();
      const docTwo = generateFakeDoc();
      docTwo.pmId = docOne.pmId;
      docTwo.title.default = docOne.title.default;
      return esClient.bulk({
        body: [
          { index: { _index: esConf.index, _type: esConf.type, _id: docOne.idConditor } },
          docOne,
          { index: { _index: esConf.index, _type: esConf.type, _id: docTwo.idConditor } },
          docTwo
        ],
        refresh: true
      }).then(() => {
        return new Promise((resolve, reject) => {
          coDeduplicate.doTheJob(docOne, function (err) {
            if (err) return reject(err);
            expect(docOne.isDuplicate).to.be.true;
            expect(docOne.duplicates).to.be.an('Array');
            expect(docOne.duplicates).to.have.lengthOf(1);
            const duplicate = docOne.duplicates[0];
            expect(duplicate.source).to.be.equal(docTwo.source);
            expect(duplicate.idConditor).to.be.equal(docTwo.idConditor);
            expect(duplicate.rules).to.be.an('Array');
            expect(duplicate.rules.length).to.be.gte(1);
            expect(duplicate.rules).to.include('Article : 1ID:pmid+TiC');
            resolve();
          });
        });
      });
    });

    it('should find a duplicate with same halId and title.default (rule 3)', function () {
      const docOne = generateFakeDoc();
      const docTwo = generateFakeDoc();
      docTwo.halId = docOne.halId;
      docTwo.title.default = docOne.title.default;
      return esClient.bulk({
        body: [
          { index: { _index: esConf.index, _type: esConf.type, _id: docOne.idConditor } },
          docOne,
          { index: { _index: esConf.index, _type: esConf.type, _id: docTwo.idConditor } },
          docTwo
        ],
        refresh: true
      }).then(() => {
        return new Promise((resolve, reject) => {
          coDeduplicate.doTheJob(docOne, function (err) {
            if (err) return reject(err);
            expect(docOne.isDuplicate).to.be.true;
            expect(docOne.duplicates).to.be.an('Array');
            expect(docOne.duplicates).to.have.lengthOf(1);
            const duplicate = docOne.duplicates[0];
            expect(duplicate.source).to.be.equal(docTwo.source);
            expect(duplicate.idConditor).to.be.equal(docTwo.idConditor);
            expect(duplicate.rules).to.be.an('Array');
            expect(duplicate.rules.length).to.be.gte(1);
            expect(duplicate.rules).to.include('Article : 1ID:halId+TiC');
            resolve();
          });
        });
      });
    });

    it('should find a duplicate with same pmId and doi (rule 5)', function () {
      const docOne = generateFakeDoc();
      const docTwo = generateFakeDoc();
      docTwo.pmId = docOne.pmId;
      docTwo.doi = docOne.doi;
      return esClient.bulk({
        body: [
          { index: { _index: esConf.index, _type: esConf.type, _id: docOne.idConditor } },
          docOne,
          { index: { _index: esConf.index, _type: esConf.type, _id: docTwo.idConditor } },
          docTwo
        ],
        refresh: true
      }).then(() => {
        return new Promise((resolve, reject) => {
          coDeduplicate.doTheJob(docOne, function (err) {
            if (err) return reject(err);
            expect(docOne.isDuplicate).to.be.true;
            expect(docOne.duplicates).to.be.an('Array');
            expect(docOne.duplicates).to.have.lengthOf(1);
            const duplicate = docOne.duplicates[0];
            expect(duplicate.source).to.be.equal(docTwo.source);
            expect(duplicate.idConditor).to.be.equal(docTwo.idConditor);
            expect(duplicate.rules).to.be.an('Array');
            expect(duplicate.rules.length).to.be.gte(1);
            expect(duplicate.rules).to.include('Article : 1ID:doi+pmid');
            resolve();
          });
        });
      });
    });

    afterEach(function () {
      return esClient.deleteByQuery({
        index: esConf.index,
        q: '*',
        refresh: true
      });
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
