const { has, pick } = require('lodash');
const EventEmitter = require('events');

const { deduplicate: { target } } = require('@istex/config-component').get(module);
const { search, update, updateDuplicatesTree } = require('./src/documentsManager');
const { buildQuery } = require('./src/deduplicateQueryBuilder');

class Business extends EventEmitter {
  doTheJob (docObject, cb) {
    deduplicate(docObject)
      .then(() => cb())
      .catch((reason) => {
        docObject.error = {
          code: reason?.code,
          message: reason?.message,
          stack: reason?.stack,
        };
        return cb(reason);
      });
  }
}

const business = new Business();

module.exports = business;

function deduplicate (docObject) {
  return Promise.resolve().then(
    () => {
      if (!has(docObject, 'technical.internalId')) {
        throw new Error('Expected Object N/A to have property technical.internalId');
      }

      // maybe change this into simple warning
      if (!has(docObject, 'business.duplicateGenre')) {
        throw new Error(`Expected Object ${docObject.technical.internalId} to have property business.duplicateGenre`);
      }

      const request = buildQuery(docObject);

      if (request.query.bool.should.length === 0) {
        business.emit('info', `Not deduplicable {docObject}, internalId: ${docObject.technical.internalId}`);
        docObject.business.isDeduplicable = false;
        return update(target,
          docObject.technical.internalId,
          { doc: { business: { isDeduplicable: false } } });
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

          docObject.business.isDuplicate = false;

          return update(target,
            docObject.technical.internalId,
            { doc: pick(docObject, ['business.isDeduplicable', 'business.isDuplicate']) });
        }

        return updateDuplicatesTree(docObject, hits.hits);
      });
    },
  );
}
