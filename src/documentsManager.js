const esClient = require('../helpers/esHelpers/client').get();
const { elastic: { indices, aliases }, deduplicate: { target } } = require('@istex/config-component').get(module);
const { _, isString, set } = require('lodash');
const { omit, get } = require('lodash/fp');
const fs = require('fs-extra');
const path = require('path');
const assert = require('assert').strict;
const { logError, logInfo, logWarning } = require('../helpers/logger');
const {
  buildSourceUidChain,
  buildDuplicatesAndSelf,
  buildDuplicateFromDocObject,
  partitionDuplicatesClusters,
  buildDuplicatesFromEsHits,
  unwrapEsHits,
  hasDuplicateFromOtherSession,
  hasDuplicate,
} = require('../helpers/deduplicates/helpers');

const validateDuplicates = fs.readFileSync(path.join(__dirname, '../painless/updateDuplicatesGraph.painless'), 'utf8');

module.exports = { search, deleteById, index, bulk, bulkCreate, update, updateByQuery, updateDuplicatesGraph };

function search ({ q, body = {}, index = '*', size }) {
  return esClient
    .search({
      q,
      body,
      index,
      size,
    });
}

function deleteById (id, index, { refresh }) {
  return esClient
    .delete({
      id,
      index,
      refresh,
    });
}

function index (index, { body, refresh }) {
  return esClient
    .index(
      {
        index,
        body,
        refresh,
      },
    );
}

function bulk ({ body }) {
  return esClient
    .bulk(
      {
        body,
      },
    );
}

function update (index, id, body, { refresh = true, ...options } = {}) {
  return esClient
    .update(
      {
        index,
        id,
        body,
        refresh,
        ...options,
      },
    );
}

function updateByQuery (index, q, body, { refresh = true, ...options } = {}) {
  return esClient
    .updateByQuery(
      {
        index,
        q,
        body,
        refresh,
        ...options,
      },
    );
}

function bulkCreate (docObjects, index, { refresh, throwOnError = false, pipeline }) {
  const bulkRequest = buildCreateBody(docObjects);
  return esClient.bulk(
    {
      index,
      body: bulkRequest,
      refresh,
      pipeline,
    })
    .then(({ body: bulkResponse }) => {
      if (bulkResponse.errors) {
        const erroredDocuments = [];
        // The items array has the same order of the dataset we just indexed.
        // The presence of the `error` key indicates that the operation
        // that we did for the document has failed.
        bulkResponse.items.forEach((action, i) => {
          const operation = Object.keys(action)[0];
          if (action[operation].error) {
            erroredDocuments.push({
              // If the status is 429 it means that you can retry the document,
              // otherwise it's very likely a mapping error, and you should
              // fix the document before to try it again.
              status: action[operation].status,
              error: action[operation].error,
              operation: bulkRequest[i * 2],
              document: bulkRequest[i * 2 + 1],
            });
          }
        });
        logError('#bulkCreate Error');
        console.dir(erroredDocuments, { depth: 2 });
        if (throwOnError === true) {
          throw new Error('#bulkCreate Error');
        }
      }
    });
}

function buildCreateBody (docObjects) {
  return _(docObjects)
    .compact()
    .transform(
      (body, docObject) => {
        const indexPayload = { _id: docObject.technical.internalId };
        body.push({ create: indexPayload });
        body.push(docObject);
      },
      [],
    )
    .value();
}

function searchBySourceUid (...sourceUids) {
  return Promise.resolve()
    .then(
      () => {
        if (_.compact(sourceUids).length === 0) return [];
        const q = `sourceUid:("${sourceUids.join('" OR "')}")`;
        return search({ q, index: target })
          .then((result) => {
            return unwrapEsHits(result?.body?.hits?.hits);
          });
      },
    );
}

function searchSubDuplicates (subDuplicateSourceUids, loadedDocumentSourceUids, accumulator = []) {
  return searchBySourceUid(...subDuplicateSourceUids)
    .then(
      (subDuplicateDocuments) => {
        const newLoadedDocumentSourceUids = _(subDuplicateSourceUids)
          .concat(loadedDocumentSourceUids)
          .uniq()
          .value();
        const newSubDuplicateSourceUids =
          _(subDuplicateDocuments)
            .flatMap('business.duplicates')
            .compact()
            .map('sourceUid')
            .uniq()
            .pull(
              ...newLoadedDocumentSourceUids,
            )
            .value();

        accumulator.push(...subDuplicateDocuments);
        accumulator.graphLevel = (accumulator.graphLevel ?? 0) + 1;

        if (newSubDuplicateSourceUids.length !== 0 && accumulator.graphLevel <= 5) {
          return searchSubDuplicates(newSubDuplicateSourceUids, newLoadedDocumentSourceUids, accumulator);
        } else {
          return accumulator;
        }
      },
    );
}

function searchDuplicatesBySourceUid (sourceUid) {
  return Promise.resolve()
    .then(() => searchBySourceUid(sourceUid))
    .then((result) => { return result?.[0]?.business?.duplicates; });
}

async function _updateDuplicatesGraph (docObject, currentSessionName, duplicateDocumentsEsHits = []) {
  assert.ok(isString(currentSessionName) && currentSessionName !== '',
    'Expect <currentSessionName> to be a not empty {string}');
  const newFoundDuplicateDocuments = unwrapEsHits(duplicateDocumentsEsHits);
  let subDuplicateDocuments = [];
  let allNotDuplicateSourceUids = [];
  let allDuplicateSourceUids = [];

  const refreshedDuplicates = await searchDuplicatesBySourceUid(docObject.sourceUid);
  _.set(docObject, 'business.duplicates', refreshedDuplicates ?? []);

  if (hasDuplicate(docObject, currentSessionName)) {
    const loadedDocumentSourceUids = _([docObject.sourceUid])
      .concat(_(newFoundDuplicateDocuments).map('sourceUid').value())
      .uniq()
      .value();

    const subDuplicateSourceUids =
      _(newFoundDuplicateDocuments)
        .flatMap('business.duplicates')
        .compact()
        .map('sourceUid')
        .concat(
          _.chain(docObject)
            .get('business.duplicates', [])
            .map('sourceUid')
            .value(),
        ).uniq()
        .pull(
          ...loadedDocumentSourceUids,
        )
        .value();

    subDuplicateDocuments = await searchSubDuplicates(subDuplicateSourceUids, loadedDocumentSourceUids);

    ({ allDuplicateSourceUids, allNotDuplicateSourceUids } =
      partitionDuplicatesClusters(
        docObject,
        newFoundDuplicateDocuments,
        subDuplicateDocuments,
        currentSessionName,
      ));
  }
  const newDuplicatesAndSelf =
    _(buildDuplicateFromDocObject(docObject, currentSessionName))
      .concat(buildDuplicatesFromEsHits(duplicateDocumentsEsHits, currentSessionName))
      .concat(
        _.chain(docObject)
          .get('business.duplicates', [])
          .value(),
      )
      .concat(
        _(newFoundDuplicateDocuments).concat(subDuplicateDocuments)
          .flatMap('business.duplicates')
          .compact()
          .map(omit('rules'))
          .value(),
      )
      .uniqBy('sourceUid')
      .pullAllWith(allNotDuplicateSourceUids,
        (duplicate, sourceUidToRemove) => duplicate.sourceUid === sourceUidToRemove)
      .map((duplicate) => {
        duplicate.sessionName = currentSessionName;
        return duplicate;
      })
      .value();

  const newSourceUids = _(newDuplicatesAndSelf).map(get('sourceUid')).sort().value();
  const newSources = _(newDuplicatesAndSelf).map(get('source')).uniq().sort().value();
  const newSourceUidChain = newSourceUids.length ? `!${newSourceUids.join('!')}!` : null;

  // Todo add already present duplicates rules
  const newDuplicateRules = _(duplicateDocumentsEsHits)
    .map(({ matched_queries: rules = [] }) => rules)
    .flatMap()
    .compact()
    .uniq()
    .sortBy()
    .value();

  const newDuplicates = _.reject(newDuplicatesAndSelf, { sourceUid: docObject.sourceUid });

  docObject.business.duplicates = newDuplicates;
  docObject.business.duplicateRules = newDuplicateRules;
  docObject.business.isDuplicate = newDuplicates.length > 0;
  docObject.business.sourceUidChain = newSourceUidChain;
  docObject.business.sources = newSources;
  docObject.technical.sessionName = currentSessionName;

  const allSourceUids = newSourceUids.concat(allNotDuplicateSourceUids);

  const q = `sourceUid:("${allSourceUids.join('" OR "')}")`;

  const painlessParams = {
    duplicatesAndSelf: newDuplicatesAndSelf,
    mainSourceUid: docObject.sourceUid,
    currentSessionName,
    sourceUidsToRemove: allNotDuplicateSourceUids,
    duplicateRules: newDuplicateRules,
    isDeduplicable: docObject.business.isDeduplicable,
  };
  const body = {
    script: {
      lang: 'painless',
      source: validateDuplicates,
      params: painlessParams,
    },
  };

  // @todo: Handle the case where less than sourceUids.length documents are updated
  return updateByQuery(target, q, body, { refresh: true })
    .then(({ body: bulkResponse }) => {
      if (bulkResponse.total !== allSourceUids.length) { logWarning(`Update diff. between targets documents: ${allSourceUids.length} and updated documents total: ${bulkResponse.total} for {docObject}, internalId: ${docObject.technical.internalId}, q=${q}`); }
    });
}

function updateDuplicatesGraph (docObject, currentSessionName, duplicateDocumentsEsHits = [], { times = 5, delay = 150 } = {}) {
  return new Promise((resolve, reject) => {
    function attempt (docObject, currentSessionName, duplicateDocumentsEsHits) {
      _updateDuplicatesGraph(docObject, currentSessionName, duplicateDocumentsEsHits)
        .then(resolve)
        .catch(async (reason) => {
          if (times === 0) return reject(reason);
          times--;
          const updatedDuplicateDocumentsEsHits = await Promise.all(duplicateDocumentsEsHits.map(async (hit) => {
            const refreshedDuplicates = await searchDuplicatesBySourceUid(hit._source.sourceUid);
            _.set(hit, '_source.business.duplicates', refreshedDuplicates ?? []);
            return hit;
          }));

          setTimeout(() => { attempt(docObject, currentSessionName, updatedDuplicateDocumentsEsHits); }, delay);
        });
    }

    attempt(docObject, currentSessionName, duplicateDocumentsEsHits);
  });
}
