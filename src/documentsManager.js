const esClient = require('../helpers/esHelpers/client').get();
const { elastic: { indices, aliases }, deduplicate: { target } } = require('@istex/config-component').get(module);
const _ = require('lodash');
const { omit, get } = require('lodash/fp');
const fs = require('fs-extra');
const path = require('path');

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

function bulkCreate (docObjects, index, { refresh }) {
  return esClient.bulk(
    {
      index,
      body: buildCreateBody(docObjects),
      refresh,
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
  const duplicates = _(duplicatesDocuments)
    .concat({ _source: docObject })
    .map(({ _source, matched_queries: rules }) => ({
      rules: rules?.sort(),
      source: _source.source,
      sourceUid: _source.sourceUid,
      sessionName: _source.technical.sessionName,
      internalId: _source.technical.internalId,
    }))
    .concat(_(duplicatesDocuments).concat({ _source: docObject }).flatMap('_source.business.duplicates').compact().map(omit('rules')).value())
    .compact()
    .uniqBy('sourceUid')
    .value();

  const sourceUids = _.map(duplicates, get('sourceUid')).sort();
  const sources = _.map(duplicates, get('source')).sort();
  const sourceUidChain = sourceUids.length ? `!${sourceUids.join('!')}!` : null;

  const duplicateRules = _(duplicatesDocuments).map(({ matched_queries: rules = [] }) => rules).flatMap().uniq().sortBy().value();

  docObject.business.duplicates = _.filter(duplicates, { sourceUid: docObject.sourceUid });
  docObject.business.duplicateRules = duplicateRules;
  docObject.business.isDuplicate = duplicates.length > 0;
  docObject.business.sourceUidChain = sourceUidChain;
  docObject.business.sources = sources;

  const q = `sourceUid:("${sourceUids.join('" OR "')}")`;
  const painlessParams = {
    duplicates,
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

  return updateByQuery(target, q, body, { refresh: true });
}
