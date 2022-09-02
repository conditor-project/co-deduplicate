const esClient = require('../helpers/esHelpers/client').get();
const { elastic: { indices, aliases }, deduplicate: { target } } = require('@istex/config-component').get(module);
const _ = require('lodash');
const { omit, get } = require('lodash/fp');
const fs = require('fs-extra');
const path = require('path');
const { logError, logInfo, logWarning } = require('../helpers/logger');
const {
  buildSourceUidChain,
  buildDuplicatesAndSelf,
  buildDuplicateFromDocObject,
  partitionDuplicatesClusters,
  buildDuplicatesFromEsHits,
  unwrapEsHits,
  hasDuplicateFromOtherSession,
} = require('../helpers/deduplicates/helpers');

const validateDuplicates = fs.readFileSync(path.join(__dirname, '../painless/updateDuplicatesTree.painless'), 'utf8');

module.exports = { search, deleteById, index, bulk, bulkCreate, update, updateByQuery, updateDuplicatesTree };

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

function update (index, id, body, { refresh = true } = {}) {
  return esClient
    .update(
      {
        index,
        id,
        body,
        refresh,
      },
    );
}

function updateByQuery (index, q, body, { refresh = true } = {}) {
  return esClient
    .updateByQuery(
      {
        index,
        q,
        body,
        refresh,
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
        if (sourceUids.length === 0) return [];
        const q = `sourceUid:("${sourceUids.join('" OR "')}")`;
        return search({ q, index: target })
          .then((result) => {
            return unwrapEsHits(result?.body?.hits?.hits);
          });
      },
    );
}

//async function updateDuplicatesTree (docObject, duplicateDocumentsEsHits, currentSessionName) {
//  if (hasDuplicateFromOtherSession(docObject)) { return handleDocumentUpdate(docObject, duplicateDocumentsEsHits, currentSessionName); }
//  const duplicatesDocuments = unwrapEsHits(duplicateDocumentsEsHits);
//
//  const newDuplicatesAndSelf =
//    _(buildDuplicateFromDocObject(docObject, currentSessionName))
//      .concat(buildDuplicatesFromEsHits(duplicateDocumentsEsHits, currentSessionName))
//      .concat(
//        _.chain(docObject)
//          .get('business.duplicates', [])
//          .value(),
//      )
//      .concat(
//        _(duplicatesDocuments)
//          .flatMap('business.duplicates')
//          .compact()
//          .map(omit('rules'))
//          .value(),
//      )
//      .uniqBy('sourceUid')
//      .map((duplicate) => {
//        duplicate.sessionName = currentSessionName;
//        return duplicate;
//      })
//      .value();
//
//  const newSourceUids = _(newDuplicatesAndSelf).map(get('sourceUid')).sort().value();
//  const newSources = _(newDuplicatesAndSelf).map(get('source')).uniq().sort().value();
//  const newSourceUidChain = newSourceUids.length ? `!${newSourceUids.join('!')}!` : null;
//
//  // console.log(newSourceUids);
//  // console.log(sourceUidsToRemove);
//  // console.log(newDuplicatesAndSelf);
//  const newDuplicateRules = _(duplicateDocumentsEsHits)
//    .map(({ matched_queries: rules = [] }) => rules)
//    .flatMap()
//    .compact()
//    .uniq()
//    .sortBy()
//    .value();
//
//  const newDuplicates = _.reject(newDuplicatesAndSelf, { sourceUid: docObject.sourceUid });
//
//  docObject.business.duplicates = newDuplicates;
//  docObject.business.duplicateRules = newDuplicateRules;
//  docObject.business.isDuplicate = newDuplicates.length > 0;
//  docObject.business.sourceUidChain = newSourceUidChain;
//  docObject.business.sources = newSources;
//  docObject.technical.sessionName = currentSessionName;
//
//  const q = `sourceUid:("${_(newSourceUids).uniq().join('" OR "')}")`;
//
//  const painlessParams = {
//    duplicatesAndSelf: newDuplicatesAndSelf,
//    mainSourceUid: docObject.sourceUid,
//    sourceUidsToRemove: [],
//    currentSessionName,
//    duplicateRules: newDuplicateRules,
//    sourceUidChain: newSourceUidChain,
//    sources: newSources,
//  };
//  const body = {
//    script: {
//      lang: 'painless',
//      source: validateDuplicates,
//      params: painlessParams,
//    },
//  };
//
//  // @todo: Handle the case where less than sourceUids.length documents are updated
//  return updateByQuery(target, q, body, { refresh: true })
//    .then(({ body: bulkResponse }) => { if (bulkResponse.total !== newDuplicatesAndSelf.length) { logWarning(`Update diff. between targets documents: ${newDuplicatesAndSelf.length} and updated documents total: ${bulkResponse.total} for {docObject}, internalId: ${docObject.technical.internalId}, q=${q}`); } });
//}

//async function handleDocumentUpdate (docObject, duplicateDocumentsEsHits, currentSessionName) {
//  const duplicatesDocuments = unwrapEsHits(duplicateDocumentsEsHits);
//  const subDuplicateSourceUids =
//    _(duplicatesDocuments)
//      .flatMap('business.duplicates')
//      .compact()
//      .map('sourceUid')
//      .concat(
//        _.chain(docObject)
//          .get('business.duplicates', [])
//          .map('sourceUid')
//          .value(),
//      ).uniq()
//      .pull(
//        ..._([docObject.sourceUid])
//          .concat(_(duplicatesDocuments).map('sourceUid').value())
//          .uniq()
//          .value(),
//      )
//      .value();
//  //
//  // console.log(docObject.sourceUid);
//  // console.log(_.map(buildDuplicatesFromEsHits(duplicateDocumentsEsHits, currentSessionName), 'sourceUid'));
//  // console.log(subDuplicateSourceUids);
//  const subDuplicateDocuments = await searchBySourceUid(...subDuplicateSourceUids);
//  const { allDuplicateSourceUids, allNotDuplicateSourceUids } =
//    partitionDuplicatesClusters(
//      docObject,
//      unwrapEsHits(duplicateDocumentsEsHits),
//      subDuplicateDocuments,
//      currentSessionName,
//    );
//
//  // console.dir({ duplicates: allDuplicateSourceUids, notDuplicates: allNotDuplicateSourceUids });
//
//  const newDuplicatesAndSelf =
//    _(buildDuplicateFromDocObject(docObject, currentSessionName))
//      .concat(buildDuplicatesFromEsHits(duplicateDocumentsEsHits, currentSessionName))
//      .concat(
//        _.chain(docObject)
//          .get('business.duplicates', [])
//          .value(),
//      )
//      .concat(
//        _(duplicatesDocuments).concat(subDuplicateDocuments)
//          .flatMap('business.duplicates')
//          .compact()
//          .map(omit('rules'))
//          .value(),
//      )
//      .uniqBy('sourceUid')
//      .pullAllWith(allNotDuplicateSourceUids, (duplicate, sourceUidToRemove) => duplicate.sourceUid === sourceUidToRemove)
//      .map((duplicate) => {
//        duplicate.sessionName = currentSessionName;
//        return duplicate;
//      })
//      .value();
//
//  // console.log(newDuplicatesAndSelf);
//  const newSourceUids = _(newDuplicatesAndSelf).map(get('sourceUid')).sort().value();
//  const newSources = _(newDuplicatesAndSelf).map(get('source')).uniq().sort().value();
//  const newSourceUidChain = newSourceUids.length ? `!${newSourceUids.join('!')}!` : null;
//
//  // console.log(newSourceUids);
//  // console.log(sourceUidsToRemove);
//  // console.log(newDuplicatesAndSelf);
//  const newDuplicateRules = _(duplicateDocumentsEsHits)
//    .map(({ matched_queries: rules = [] }) => rules)
//    .flatMap()
//    .compact()
//    .uniq()
//    .sortBy()
//    .value();
//
//  const newDuplicates = _.reject(newDuplicatesAndSelf, { sourceUid: docObject.sourceUid });
//
//  docObject.business.duplicates = newDuplicates;
//  docObject.business.duplicateRules = newDuplicateRules;
//  docObject.business.isDuplicate = newDuplicates.length > 0;
//  docObject.business.sourceUidChain = newSourceUidChain;
//  docObject.business.sources = newSources;
//  docObject.technical.sessionName = currentSessionName;
//
//  const allSourceUids = allDuplicateSourceUids.concat(allNotDuplicateSourceUids);
//  const q = `sourceUid:("${allSourceUids.join('" OR "')}")`;
//
//  const painlessParams = {
//    duplicatesAndSelf: newDuplicatesAndSelf,
//    mainSourceUid: docObject.sourceUid,
//    currentSessionName,
//    sourceUidsToRemove: allNotDuplicateSourceUids,
//    duplicateRules: newDuplicateRules,
//    sourceUidChain: newSourceUidChain,
//    sources: newSources,
//  };
//  const body = {
//    script: {
//      lang: 'painless',
//      source: validateDuplicates,
//      params: painlessParams,
//    },
//  };
//
//  // @todo: Handle the case where less than sourceUids.length documents are updated
//  return updateByQuery(target, q, body, { refresh: true })
//    .then(({ body: bulkResponse }) => { if (bulkResponse.total !== allSourceUids.length) { logWarning(`Update diff. between targets documents: ${allSourceUids.length} and updated documents total: ${bulkResponse.total} for {docObject}, internalId: ${docObject.technical.internalId}, q=${q}`); } });
//}

async function updateDuplicatesTree  (docObject, duplicateDocumentsEsHits, currentSessionName) {
  const duplicatesDocuments = unwrapEsHits(duplicateDocumentsEsHits);
  let subDuplicateDocuments = [];
  let allNotDuplicateSourceUids = [];
  let allDuplicateSourceUids = [];

  if (hasDuplicateFromOtherSession(docObject)) {
    const subDuplicateSourceUids =
      _(duplicatesDocuments)
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
          ..._([docObject.sourceUid])
            .concat(_(duplicatesDocuments).map('sourceUid').value())
            .uniq()
            .value(),
        )
        .value();

    subDuplicateDocuments = await searchBySourceUid(...subDuplicateSourceUids);

    ({ allDuplicateSourceUids, allNotDuplicateSourceUids } =
      partitionDuplicatesClusters(
        docObject,
        duplicatesDocuments,
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
        _(duplicatesDocuments).concat(subDuplicateDocuments)
          .flatMap('business.duplicates')
          .compact()
          .map(omit('rules'))
          .value(),
      )
      .uniqBy('sourceUid')
      .pullAllWith(allNotDuplicateSourceUids, (duplicate, sourceUidToRemove) => duplicate.sourceUid === sourceUidToRemove)
      .map((duplicate) => {
        duplicate.sessionName = currentSessionName;
        return duplicate;
      })
      .value();

  const newSourceUids = _(newDuplicatesAndSelf).map(get('sourceUid')).sort().value();
  const newSources = _(newDuplicatesAndSelf).map(get('source')).uniq().sort().value();
  const newSourceUidChain = newSourceUids.length ? `!${newSourceUids.join('!')}!` : null;

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
    sourceUidChain: newSourceUidChain,
    sources: newSources,
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
    .then(({ body: bulkResponse }) => { if (bulkResponse.total !== allSourceUids.length) { logWarning(`Update diff. between targets documents: ${allSourceUids.length} and updated documents total: ${bulkResponse.total} for {docObject}, internalId: ${docObject.technical.internalId}, q=${q}`); } });
}
