const should = require('should');
const assert = require('chai').assert;
const { search } = require('../../src/documentsManager');
const deleteIndiceIx = require('../../helpers/esHelpers/deleteIndiceIx');
const createIndiceNx = require('../../helpers/esHelpers/createIndiceNx');
const { putCreationAndModificationDatePipeline, deleteCreationAndModificationDatePipeline } = require(
  '../../helpers/esHelpers/putCreationAndModificationDatePipeline');
const { elastic: { indices }, deduplicate: { target } } = require('@istex/config-component').get(module);
const { elastic: { mapping } } = require('corhal-config');
const business = require('../../index');
const { doTheJob } = business;
const { notDuplicatesFixtures } = require('./dataset/notDuplicatesFixtures');
const { duplicatesFixtures } = require('./dataset/duplicatesFixtures');
const { bulkCreate } = require('../../src/documentsManager');
const { _, reject, filter, some, find, isEmpty } = require('lodash');
const { get: fpGet } = require('lodash/fp');
const { logInfo, logError } = require('../../helpers/logger');
const { hasDuplicateFromOtherSession, hasOwnDuplicateFromOtherSession, buildSourceUidChain } = require('../../helpers/deduplicates/helpers');

const TEST_SESSION = 'TEST_SESSION';
notDuplicatesFixtures
  .concat(duplicatesFixtures)
  .forEach((docObject) => {
    _.set(docObject, 'technical.sessionName', TEST_SESSION);
  });

logInfo('Total of documents: ' + (duplicatesFixtures.length + notDuplicatesFixtures.length));

before(function () {
  this.timeout(10000);
  return deleteIndiceIx(indices.documents.index)
    .then(() => createIndiceNx(
      indices.documents.index,
      { mappings: mapping.mappings, settings: mapping.settings, aliases: indices.documents.aliases },
    ))
    .then(() => putCreationAndModificationDatePipeline())
    .then(() => bulkCreate(notDuplicatesFixtures, indices.documents.index, { refresh: true, throwOnError: true }))
    .then(() => bulkCreate(duplicatesFixtures, indices.documents.index, { refresh: true, throwOnError: true }));
});

// after(function () {
//  return deleteIndiceIx(indices.documents.index)
//    .then(() => deleteCreationAndModificationDatePipeline());
// });

business.on('info', (message) => logInfo(message));

describe('doTheJob', function () {
  notDuplicatesFixtures.forEach((docObjectNoDuplicates) => {
    it(`Must not find duplicates for docObject ${docObjectNoDuplicates.technical.internalId}`, (done) => {
      doTheJob(docObjectNoDuplicates, (err) => {
        if (err) return done(err);
        docObjectNoDuplicates.business.should.have.property('isDeduplicable');
        docObjectNoDuplicates.business.should.have.property('isDuplicate').equal(false);
        done();
      });
    });
  });

  // Test every docObjectWithDuplicates exept 'crossref$10.1001/jama.2014.15912', 'b$5'
  reject(duplicatesFixtures, (docObject) => ['crossref$10.1001/jama.2014.15912', 'b$5'].includes(docObject.sourceUid))
    .forEach((docObjectWithDuplicates) => {
      it(`Must find duplicates for docObject ${docObjectWithDuplicates.technical.internalId}`, (done) => {
        doTheJob(docObjectWithDuplicates, (err) => {
          if (err) return done(err);

          const expectedSourcesLength = _(docObjectWithDuplicates.business.duplicates)
            .map(fpGet('source'))
            .uniq()
            .size() + 1;

          docObjectWithDuplicates.business.should.have.property('isDuplicate').equal(true);
          docObjectWithDuplicates.business.should.have.property('isDeduplicable').equal(true);
          docObjectWithDuplicates.business.should.have.property('sources').with.lengthOf(expectedSourcesLength);
          docObjectWithDuplicates.business.sourceUidChain.should.equal(buildSourceUidChain(docObjectWithDuplicates));
          assert.isNotTrue(hasDuplicateFromOtherSession(docObjectWithDuplicates), 'Expect no duplicate from other session');
          done();
        });
      });
    });
});

const crossrefSourceuid = 'crossref$10.1001/jama.2014.15912';
const pubmedSourceUid  = 'pubmed$25603006';

const docObjectCrossref = find(duplicatesFixtures, (docObject) => docObject.sourceUid === crossrefSourceuid);
describe(`For the docObject sourceUid:${crossrefSourceuid}`, function () {
  it('Must find {duplicates}', function (done) {
    this.timeout(50000);
    doTheJob(docObjectCrossref, (err) => {
      if (err) return done(err);

      const expectedSourcesLength = _(docObjectCrossref.business.duplicates).map(fpGet('source')).uniq().size() + 1;

      docObjectCrossref.business.should.have.property('isDuplicate').equal(true);
      docObjectCrossref.business.should.have.property('isDeduplicable').equal(true);
      docObjectCrossref.business.should.have.property('sources').with.lengthOf(expectedSourcesLength);
      docObjectCrossref.business.sourceUidChain.should.equal(buildSourceUidChain(docObjectCrossref));
      assert.isNotTrue(hasDuplicateFromOtherSession(docObjectCrossref), 'Expect no  duplicate from other session');
      docObjectCrossref.business.duplicates.should.containEql({
        sessionName: 'TEST_SESSION',
        source: 'k',
        sourceUid: 'k$1',
      });
      docObjectCrossref.business.duplicates.should.not.containEql({
        sessionName: 'TEST_SESSION',
        source: 'h',
        sourceUid: 'h$1',
      });
      docObjectCrossref.business.duplicates.should.not.containEql({
        sessionName: 'ANOTHER_SESSION',
        source: 'h',
        sourceUid: 'h$1',
      });

      done();
    });
  });

  it('Must update duplicates of "hal$hal-02462375"', function () {
    return search({ q: 'sourceUid:"hal$hal-02462375"', index: target })
      .then((result) => {
        const docObjectHal = result.body.hits.hits[0]._source;
        const duplicates = docObjectHal.business.duplicates;

        duplicates.should.be.Array();
        duplicates.should.not.containEql({
          sourceUid: 'crossref$10.1001/jama.2014.15912',
          sessionName: 'ANOTHER_SESSION',
          rules: [
            'RULE_555',
          ],
          source: 'crossref',
        });
        duplicates.should.not.containEql({
          sessionName: 'ANOTHER_SESSION',
          source: 'x',
          sourceUid: 'x$1',
        });
        docObjectHal.business.sourceUidChain.should.equal(buildSourceUidChain(docObjectHal));
      });
  });

  it('Must update duplicates of "pubmed$25603006"', function () {
    return search({ q: 'sourceUid:"pubmed$25603006"', index: target })
      .then((result) => {
        const docObjectPubmed = result.body.hits.hits[0]._source;
        const duplicates = docObjectPubmed.business.duplicates;

        const duplicateCrossref = find(duplicates, (duplicate) => duplicate.sourceUid === crossrefSourceuid);
        duplicates.should.be.Array();
        duplicateCrossref.should.containEql({
          sourceUid: 'crossref$10.1001/jama.2014.15912',
          internalId: 'QVtJr9XOWFjIcbXWTLSXfZGN5',
          sessionName: 'TEST_SESSION',
          source: 'crossref',
        });
        duplicateCrossref.should.have.property('rules');
        duplicateCrossref.rules.should.be.an.Array().and.not.be.empty();

        duplicates.should.containEql({
          sourceUid: 'x$1',
          sessionName: 'TEST_SESSION',
          source: 'x',
          rules: ['RULE_111'],
        });
        docObjectPubmed.business.sourceUidChain.should.equal(buildSourceUidChain(docObjectPubmed));
      });
  });

  it('Must update duplicates of "b$5"', function () {
    return search({ q: 'sourceUid:"b$5"', index: target })
      .then((result) => {
        const docObjectB = result.body.hits.hits[0]._source;
        const duplicates = docObjectB.business.duplicates;

        duplicates.should.not.containEql({
          sessionName: 'TEST_SESSION',
          source: 'h',
          sourceUid: 'h$1',
        });

        duplicates.should.not.containEql({
          sessionName: 'ANOTHER_SESSION',
          source: 'h',
          sourceUid: 'h$1',
        });

        docObjectB.business.sourceUidChain.should.equal(buildSourceUidChain(docObjectB));
      });
  });
});
