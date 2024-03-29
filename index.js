const { has } = require('lodash');
const EventEmitter = require('events');

const { deduplicate: { target }, currentSessionName } = require('@istex/config-component').get(module);
const { search, updateDuplicatesGraph, searchIgnoredBySourceUid } = require('./src/documentsManager');
const { buildQuery } = require('./src/deduplicateQueryBuilder');

class Business extends EventEmitter {
  doTheJob (docObject, cb) {
    if (typeof cb === 'function') {
      deduplicate(docObject)
        .then(() => cb())
        .catch((reason) => {
          _setDocObjectError(docObject, reason);
          return cb(reason);
        });
    } else {
      return deduplicate(docObject)
        .catch((reason) => {
          _setDocObjectError(docObject, reason);
          throw reason;
        });
    }
  }
}

const business = new Business();

module.exports = business;

function deduplicate (docObject) {
  return Promise.resolve().then(
    async () => {
      if (!has(docObject, 'technical.internalId')) {
        throw new Error('Expected Object N/A to have property technical.internalId');
      }

      // maybe change this into simple warning
      if (!has(docObject, 'business.duplicateGenre')) {
        throw new Error(`Expected Object ${docObject.technical.internalId} to have property business.duplicateGenre`);
      }
      const ignoredFields = await searchIgnoredBySourceUid(docObject.sourceUid);
      const request = buildQuery(docObject, ignoredFields);

      if (request.query.bool.filter.bool.should.length === 0) {
        business.emit('info', `Not deduplicable {docObject}, internalId: ${docObject.technical.internalId}`);
        docObject.business.isDeduplicable = false;
        return updateDuplicatesGraph(docObject, currentSessionName);
      }

      docObject.business.isDeduplicable = true;

      return search({
        index: target,
        body: request,
        size: 1000, // This means, 1000 duplicates found max, hopefully it would be enougth.
      }).then((result) => {
        const { body: { hits } } = result;
        if (hits.total.value === 0) {
          business.emit('info',
            `No duplicates found for {docObject}, internalId: ${docObject.technical.internalId}`);
        }

        return updateDuplicatesGraph(docObject, currentSessionName, hits.hits);
      });
    },
  );
}

function _setDocObjectError (docObject, error) {
  docObject.error = {
    code: error?.code ?? error?.meta?.statusCode,
    message: error?.message,
    stack: error?.stack,
    failuresList: error?.failuresList,
    failuresTypes: error?.failuresTypes,
  };
  return docObject;
}
