const should = require('should');
const deleteIndiceIx = require('../../helpers/esHelpers/deleteIndiceIx');
const createIndiceNx = require('../../helpers/esHelpers/createIndiceNx');
const { putCreationAndModificationDatePipeline, deleteCreationAndModificationDatePipeline } = require('../../helpers/esHelpers/putCreationAndModificationDatePipeline');
const { elastic: { indices } } = require('@istex/config-component').get(module);
const { elastic: { mapping } } = require('corhal-config');
const business = require('../../index');
const { doTheJob } = business;
const { notDuplicatesFixtures } = require('./dataset/notDuplicatesFixtures');
const { duplicatesFixtures } = require('./dataset/duplicatesFixtures');
const { bulkCreate } = require('../../src/documentsManager');
const _ = require('lodash');
const { logInfo } = require('../../helpers/logger');

notDuplicatesFixtures
  .concat(duplicatesFixtures)
  .forEach((docObject) => {
    _.set(docObject, 'technical.sessionName', 'TEST_SESSION');
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

after(function () {
  return deleteIndiceIx(indices.documents.index)
    .then(() => deleteCreationAndModificationDatePipeline());
});

business.on('info', (message) => logInfo(message));

describe('doTheJob', function () {
  notDuplicatesFixtures.forEach((docObjectNoDuplicates) => {
    it(`Must not find duplicates for docObject ${docObjectNoDuplicates.technical.internalId}`, (done) => {
      doTheJob(docObjectNoDuplicates, (err) => {
        if (err) return done(err);
        docObjectNoDuplicates.business.should.have.property('isDuplicate').equal(false);
        done();
      });
    });
  });

  duplicatesFixtures.forEach((docObjectWithDuplicates) => {
    it(`Must find duplicates for docObject ${docObjectWithDuplicates.technical.internalId}`, (done) => {
      doTheJob(docObjectWithDuplicates, (err) => {
        if (err) return done(err);
        docObjectWithDuplicates.business.should.have.property('isDuplicate').equal(true);
        done();
      });
    });
  });
});
