const esClient = require('../helpers/esHelpers/client').get();
const { elastic: { indices, aliases } } = require('@istex/config-component').get(module);
const _ = require('lodash');
const { omit, get } = require('lodash/fp');
const fs = require('fs-extra');
const path = require('path');

const validateDuplicates = fs.readFileSync(path.join(__dirname, '../painless/validateDuplicates.painless'), 'utf8');

module.exports = { search, deleteById, index, bulk, bulkCreate, update, aggregate, updateByQuery, updateDuplicatesTree };

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
        pipeline: 'set_creation_and_modification_date',
      },
    );
}

function bulkCreate (docObjects, index, { refresh }) {
  return esClient.bulk(
    {
      index,
      body: buildCreateBody(docObjects),
      refresh,
      pipeline: 'set_creation_and_modification_date',
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
      rules,
      source: _source.source,
      sourceUid: _source.sourceUid,
      sessionName: _source.technical.sessionName,
      internalId: _source.technical.internalId,
    }))
    .concat(_(duplicatesDocuments).flatMap('_source.business.duplicates').compact().map(omit('rules')).value())
    .compact()
    .uniqBy('sourceUid')
    .value();

  const sourceUids = _.map(duplicates, get('sourceUid')).sort();
  const q = `sourceUid:("${sourceUids.join('" OR "')}")`;

  const painlessParams = {
    duplicates,
    sourceUidChain: sourceUids.length ? `!${sourceUids.join('!')}!` : null,
  };
console.dir(painlessParams)
  const params = {
    q,
    index: indices.documents.index,
    body: {
      script: {
        lang: 'painless',
        source: validateDuplicates,
        params: painlessParams,
      },
    },
    refresh: true,
  };

  return esClient
    .updateByQuery(params)
  ;
}

function aggregate (docObject, hits) {
  const duplicates = [];
  let duplicateRules = [];
  let duplicatesSourceUids = [];

  _.each(hits, (hit) => {
    duplicates.push({
      rules: hit.matched_queries,
      source: hit._source.source,
      sourceUid: hit._source.sourceUid,
      sessionName: hit._source.technical.sessionName,
      internalId: hit._source.technical.internalId,
    });

    const sourceUids =
      _.chain(hit)
        .get('_source.business.sourceUidChain', '')
        .split('!')
        .pull('')
        .value();

    duplicatesSourceUids = _.union(duplicatesSourceUids, sourceUids);
    duplicateRules = _.union(duplicateRules, hit.matched_queries);
  });

  _.compact(duplicatesSourceUids);

  docObject.business.duplicates = duplicates;
  docObject.business.duplicateRules = _.sortBy(duplicateRules);
  docObject.business.isDuplicate = duplicates.length > 0;
  docObject.business.sourceUidChain = `!${duplicatesSourceUids.concat([docObject.sourceUid]).sort().join('!')}!`;
  docObject.technical.modificationDate = new Date().getTime();

  const body = {
    doc: _.pick(
      docObject,
      ['business.duplicates', 'business.duplicatesRules', 'business.isDuplicate', 'business.sourceUidChain', 'technical.modificationDate']),
  };

  return update(aliases.TO_DEDUPLICATE, docObject.technical.internalId, body);
}
