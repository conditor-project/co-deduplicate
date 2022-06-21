const esClient = require('../helpers/esHelpers/client').get();
const { elastic: { indices, aliases }, deduplicate: { target } } = require('@istex/config-component').get(module);
const _ = require('lodash');
const { omit, get } = require('lodash/fp');
const fs = require('fs-extra');
const path = require('path');
const { logError, logInfo } = require('../helpers/logger');
const { buildSourceUidChain, buildDuplicatesAndSelf } = require('../helpers/deduplicates/helpers');

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

function bulkCreate (docObjects, index, { refresh, throwOnError = false }) {
  const bulkRequest = buildCreateBody(docObjects);
  return esClient.bulk(
    {
      index,
      body: bulkRequest,
      refresh,
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
    .value()
  ;
}

function updateDuplicatesTree (docObject, duplicatesDocuments) {
  const sourceUidsToRemove =
    _.chain(docObject)
      .get('business.duplicates')
      .filter(duplicate => duplicate.sessionName !== docObject.technical.sessionName)
      .map((duplicate) => duplicate.sourceUid)
      .push(docObject.sourceUid)
      .value();

  const newDuplicatesAndSelf =
    _(duplicatesDocuments)
      .concat({ _source: docObject })
      .map(({ _source, matched_queries: rules }) => ({
        rules: rules?.sort(),
        source: _source.source,
        sourceUid: _source.sourceUid,
        sessionName: docObject?.technical?.sessionName,
        internalId: _source.technical.internalId,
      }))
      .concat(
        _.chain(docObject)
          .get('business.duplicates')
          .reject(duplicate => duplicate.sessionName !== docObject.technical.sessionName)
          .value(),
      )
      .concat(
        _(duplicatesDocuments)
          .flatMap('_source.business.duplicates')
          .compact()
          .reject(duplicate =>
            duplicate.sessionName !== docObject.technical.sessionName &&
                               sourceUidsToRemove.includes(duplicate.sourceUid) &&
                               _.isEmpty(duplicate.rules),
          )
          .map(omit('rules'))
          .map((duplicate) => { duplicate.sessionName = docObject.technical.sessionName; return duplicate; })
          .value(),
      )
      .compact()
      .uniqBy('sourceUid')
      .value();

  const newSourceUids = _(newDuplicatesAndSelf).map(get('sourceUid')).sort().value();
  const newSources = _(newDuplicatesAndSelf).map(get('source')).uniq().sort().value();
  const newSourceUidChain = newSourceUids.length ? `!${newSourceUids.join('!')}!` : null;
  //console.log(sourceUidsToRemove);
  //console.log(newDuplicatesAndSelf);
  const newDuplicateRules = _(duplicatesDocuments).map(({ matched_queries: rules = [] }) => rules).flatMap().uniq().sortBy().value();

  docObject.business.duplicates = _.reject(newDuplicatesAndSelf, { sourceUid: docObject.sourceUid });
  docObject.business.duplicateRules = newDuplicateRules;
  docObject.business.isDuplicate = newDuplicatesAndSelf.length > 0;
  docObject.business.sourceUidChain = newSourceUidChain;
  docObject.business.sources = newSources;

  const q = `sourceUid:("${_(newSourceUids).concat(sourceUidsToRemove).uniq().join('" OR "')}")`;

  const painlessParams = {
    duplicatesAndSelf: newDuplicatesAndSelf,
    selfSourceUid: docObject.sourceUid,
    currentSessionName: docObject?.technical?.sessionName,
    sourceUidsToRemove,
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
    .then(({ body: bulkResponse }) => { if (bulkResponse.total !== newDuplicatesAndSelf.length) { logInfo(`Update diff. between targets documents: ${newDuplicatesAndSelf.length} and updated documents total: ${bulkResponse.total} for {docObject}, internalId: ${docObject.technical.internalId}, q=${q}`); } });
}
