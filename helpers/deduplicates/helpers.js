const { _, get, isPlainObject, each, isEmpty } = require('lodash');
const fp = require('lodash/fp');
const assert = require('assert').strict;
const createGraph = require('ngraph.graph');
const detectClusters = require('ngraph.louvain');
const coarsen = require('ngraph.coarsen');
const path = require('ngraph.path');

const DUPLICATES_PATH = 'business.duplicates';
const SOURCE_UID_PATH = 'sourceUid';
const SOURCE_PATH = 'source';
const INTERNAL_ID_PATH = 'technical.internalId';
const SESSION_NAME = 'technical.sessionName';

module.exports = {
  buildDuplicatesBucket,
  buildDuplicate,
  buildDuplicateFromDocObject,
  buildDuplicatesAndSelf,
  hasDuplicateFromOtherSession,
  hasOwnDuplicateFromOtherSession,
  hasTransDuplicateFromOtherSession,
  buildSourceUidChain,
  partitionDuplicatesClusters,
  buildDuplicatesFromEsHits,
  buildSources,
  unwrapEsHits,
};

/*
    Duplicates {array}
 */
function buildDuplicatesBucket (docObject) {
  assert.ok(isPlainObject(docObject), 'Expect <docObject> to be a plain {object}');
  const duplicates = _.chain(docObject).get(DUPLICATES_PATH).map(fp.omit(['rules', 'sessionName'])).value();
  const self = buildDuplicateFromDocObject(docObject);
  duplicates.push(self);

  return duplicates;
}

function partitionDuplicatesClusters (docObject, duplicatesDocuments = [], subDuplicateDocuments = [], currentSessionName = 'None') {
  const g = createGraph();
  g.addNode(docObject.sourceUid);

  each(duplicatesDocuments, (document) => {
    g.addLink(docObject.sourceUid, document.sourceUid);
  });

  _.chain(docObject)
    .get(DUPLICATES_PATH)
    .each((duplicate) => {
      //console.log(duplicate)
      // sessionName should be handled diff
      if (duplicate.sessionName !== currentSessionName) return g.addNode(duplicate.sourceUid);
      if (isEmpty(duplicate.rules)) {
        g.addNode(duplicate.sourceUid);
      } else {
        g.addLink(docObject.sourceUid, duplicate.sourceUid);
      }
    })
    .value();

  each(duplicatesDocuments.concat(subDuplicateDocuments), (document) => {
    _.chain(document)
      .get(DUPLICATES_PATH)
      .each((duplicate) => {
        //console.log(duplicate)
        if (duplicate.sourceUid === docObject.sourceUid) return;
        if (isEmpty(duplicate.rules)) {
          g.addNode(duplicate.sourceUid);
        } else {
          g.addLink(document.sourceUid, duplicate.sourceUid);
        }
      })
      .value();
  });

  const pathFinder = path.aStar(g);
  const result = {
    allDuplicateSourceUids: [],
    allNotDuplicateSourceUids: [],
  };

  g.forEachNode(function (node) {
    if (pathFinder.find(node.id, docObject.sourceUid).length > 0) {
      result.allDuplicateSourceUids.push(node.id);
    } else {
      result.allNotDuplicateSourceUids.push(node.id);
    }
  });

  return result;
}

// build duplicates from the docObject stand point
function buildDuplicatesAndSelf (docObject, sessionName) {
  assert.ok(isPlainObject(docObject), 'Expect <docObject> to be a plain {object}');

  const duplicates = get(docObject, DUPLICATES_PATH, []);
  const self = buildDuplicateFromDocObject(docObject, sessionName);
  duplicates.push(self);

  return duplicates;
}

function buildDuplicatesFromEsHits (hits, sessionName) {
  return _(hits)
    .map(({ _source: { sourceUid, source, technical }, matched_queries: rules }) => buildDuplicate(sourceUid,
      { source, rules, internalId: technical?.internalId, sessionName }))
    .value();
}

// Duplicates behaviors
function hasDuplicateFromOtherSession (docObject) {
  return _.chain(docObject)
    .get(DUPLICATES_PATH)
    .some((duplicate) => duplicate.sessionName !== get(docObject, 'technical.sessionName'))
    .value();
}

function hasOwnDuplicateFromOtherSession (docObject) {
  return _.chain(docObject)
    .get(DUPLICATES_PATH)
    .some((duplicate) => duplicate.sessionName !== get(docObject, 'technical.sessionName') && !isEmpty(duplicate.rules))
    .value();
}

function hasTransDuplicateFromOtherSession (docObject) {
  return _.chain(docObject)
    .get(DUPLICATES_PATH)
    .some((duplicate) => duplicate.sessionName !== get(docObject, 'technical.sessionName') && isEmpty(duplicate.rules))
    .value();
}

function buildSourceUidChain (docObject) {
  return `!${_.chain(docObject)
    .get(DUPLICATES_PATH)
    .map(fp.get('sourceUid'))
    .concat(get(docObject, SOURCE_UID_PATH))
    .uniq()
    .sort()
    .join('!')
    .value()}!`;
}

function buildSources (docObject) {
  return _.chain(docObject)
    .get(DUPLICATES_PATH)
    .map(fp.get('source'))
    .concat(get(docObject, SOURCE_PATH))
    .uniq()
    .sort()
    .value();
}

/*
 Duplicate {object}
 */
function buildDuplicate (sourceUid, { internalId, rules = [], sessionName, source }) {
  assert.strictEqual(typeof sourceUid, 'string', 'Expect <sourceUid> to be a {string}');

  return { sourceUid, internalId, rules: rules?.sort(), sessionName, source };
}

function buildDuplicateFromDocObject (docObject, sessionName, rules) {
  return buildDuplicate(
    get(docObject, SOURCE_UID_PATH),
    {
      internalId: get(docObject, INTERNAL_ID_PATH),
      source: get(docObject, SOURCE_PATH),
      sessionName,
      rules,
    },
  );
}

function unwrapEsHits (hits) {
  return hits.map(unwrapEsHit);
}

function unwrapEsHit (hit) {
  return hit._source;
}
