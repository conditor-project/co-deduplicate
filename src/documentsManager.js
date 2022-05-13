const esClient = require('../helpers/esHelpers/client').get();
const { elastic: { indices, aliases }, deduplicate: { target } } = require('@istex/config-component').get(module);
const _ = require('lodash');
const { omit, get } = require('lodash/fp');
const fs = require('fs-extra');
const path = require('path');
const { logError } = require('../helpers/logger');

const validateDuplicates = fs.readFileSync(path.join(__dirname, '../painless/updateDuplicatesTree.painless'), 'utf8');

module.exports = { search, deleteById, index, bulk, bulkCreate, update, updateByQuery, updateDuplicatesTree };

function search ({ body = {}, index = '*', size }) {
  return esClient
    .search({
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
  const duplicatesBucket =
    _(duplicatesDocuments)
      .concat({ _source: docObject })
      .map(({ _source, matched_queries: rules }) => ({
        rules: rules?.sort(),
        source: _source.source,
        sourceUid: _source.sourceUid,
        sessionName: docObject?.technical?.sessionName,
        internalId: _source.technical.internalId,
      }))
      .concat(_.get(docObject, 'business.duplicates'))
      .concat(_(duplicatesDocuments).flatMap('_source.business.duplicates').compact().map(omit('rules')).value())
      .compact()
      .uniqBy('sourceUid')
      .value();

  const sourceUids = _.map(duplicatesBucket, get('sourceUid')).sort();
  const sources = _.map(duplicatesBucket, get('source')).sort();
  const sourceUidChain = sourceUids.length ? `!${sourceUids.join('!')}!` : null;

  const duplicateRules = _(duplicatesDocuments).map(({ matched_queries: rules = [] }) => rules).flatMap().uniq().sortBy().value();

  docObject.business.duplicates = _.filter(duplicatesBucket, { sourceUid: docObject.sourceUid });
  docObject.business.duplicateRules = duplicateRules;
  docObject.business.isDuplicate = duplicatesBucket.length > 0;
  docObject.business.sourceUidChain = sourceUidChain;
  docObject.business.sources = sources;

  const q = `sourceUid:("${sourceUids.join('" OR "')}")`;

  const painlessParams = {
    duplicatesBucket,
    sourceUidChain,
    sources,
    initialSourceUid: docObject.sourceUid,
    duplicateRules,
  };
  const body = {
    script: {
      lang: 'painless',
      source: validateDuplicates,
      params: painlessParams,
    },
  };

  // @todo: Handle the case where less than sourceUids.length documents are updated
  return updateByQuery(target, q, body, { refresh: true });
}
