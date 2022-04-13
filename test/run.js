const rewire = require('rewire');
const business = rewire('../index.js');
const testData = require('./dataset/in/test.json');
const chai = require('chai');
const debug = require('debug')('test');
const expect = chai.expect;
const _ = require('lodash');
const { logError } = require('../helpers/logger');

const baseRequest = require('co-config/base_request.json');
const esClient = require('../helpers/esHelpers/client').get();
const { app, elastic: { indices } } = require('@istex/config-component').get(module);
const { elastic: { mapping } } = require('corhal-config');
const deleteIndiceIx = require('../helpers/esHelpers/deleteIndiceIx');
const createIndiceNx = require('../helpers/esHelpers/createIndiceNx');
const { search } = require('../src/documentsManager');

// after(function () {
//  return deleteIndiceIx('co-deduplicate-integration-test')
// });
describe(app.name + '/index.js', function () {
  before(function () {
    this.timeout(10000);
    return deleteIndiceIx(indices.documents.index)
      .then(() => {
        return createIndiceNx(indices.documents.index,
          { mappings: mapping.mappings, settings: mapping.settings, aliases: indices.documents.aliases });
      });
  });

  describe('#fonction loadScripts', function () {
    it('devrait lire les fichiers de script et reconstituer l\'objet scriptList', (done) => {
      const scriptList = business.__get__('loadPainlessScripts')();
      debug(Object.keys(scriptList));
      expect(Object.keys(scriptList).length, 'il devrait y avoir au moins 7 scripts painless dans la liste')
        .to
        .be
        .gte(7);
      const expectedScripts = ['addDuplicate',
        'addEmptyDuplicate',
        'removeDuplicate',
        'setDuplicateRules',
        'setHasTransDuplicate',
        'setIdChain',
        'setIsDuplicate'];
      expect(_.intersection(expectedScripts, Object.keys(scriptList)).length,
        'Les au moins 7 scripts painless doivent avoir le bon nom').to.be.gte(7);
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
  describe.only('#insert notice 1', function () {
    let totalExpected = 0;
    testData.forEach((data, index) => {
    // const index = 1;
    // const data = testData[index];
      it.only(data._comment, function (done) {
        business.doTheJob(data, function (err) {
          if (err) return done(err);
          search({
            index: indices.documents.target,
          }).then(({ body: { hits } }) => {
            if (index !== 7) totalExpected++; // erreur normale sur le 7ème doc
            expect(hits.total.value).to.be.equal(totalExpected);
            expect(hits.hits[0]._source.idConditor).not.to.be.undefined;
            expect(hits.hits[0]._source.sourceUid).not.to.be.undefined;
            hits.hits.forEach(({ _source }) => {
              if (_source.sourceUid === 'crossref$10.1021/jz502360c') {
                expect(_source.isDuplicate, 'isDuplicate doit valoir true').to.be.equal(true);
                expect(_source.duplicates.length, 'le tableau duplicates doit contenir au moins un élément').to.be.gte(1);
                expect(_source.duplicates.length).to.be.gte(1);
                expect(_source.duplicates[0].idConditor === 'Qd74UnItx6nGYLwrBc2MDZF8k');
                expect(_source.duplicates[0].rules[0].indexOf('2Collation'),
                  'doit matcher avec la règle 2Collation...').to.be.gte(0);
              }
            });
            done();
          }).catch(done);
        });
      });
    });
  });

  describe('#tests des normalizer', function () {
    it('Titre normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: indices.documents.index,
        body: {
          field: 'title.default.normalized',
          text: 'Voici un test de titre caparaçonner aïoli ! ',
        },
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('voiciuntestdetitrecaparaconneraioli');
        done();
      });
    });

    it('Auteur normalizer retourne la bonne valeur', function (done) {
      esClient.indices.analyze({
        index: indices.documents.index,
        body: {
          field: 'first3AuthorNames.normalized',
          text: 'Gérard Philippe, André Gide',
        },
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
          field: 'doi.normalized',
          text: '1586-544984Efrea',
        },
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
          field: 'pageRange.normalized',
          text: '158-165',
        },
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
          field: 'volume.normalized',
          text: 'v52',
        },
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
          field: 'issue.normalized',
          text: 'V14',
        },
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
          field: 'publicationDate.normalized',
          text: '18-11-2012',
        },
      }, function (esError, response) {
        expect(esError).to.be.undefined;
        expect(response).to.not.be.undefined;
        expect(response.tokens[0].token).to.be.equal('2012');
        done();
      });
    });
  });
});
