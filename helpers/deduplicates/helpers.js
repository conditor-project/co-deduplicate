const { _, get, isPlainObject, some, isEmpty } = require('lodash');
const fp = require('lodash/fp');
const assert = require('assert').strict;

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

// build duplicates from the docObject stand point
function buildDuplicatesAndSelf (docObject) {
  assert.ok(isPlainObject(docObject), 'Expect <docObject> to be a plain {object}');

  const duplicates = get(docObject, DUPLICATES_PATH, []);
  const self = buildDuplicateFromDocObject(docObject);
  duplicates.push(self);

  return duplicates;
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
  return `!${_.chain(docObject).get(DUPLICATES_PATH).map(fp.get('sourceUid')).concat(get(docObject, SOURCE_UID_PATH)).uniq().sort().join('!').value()}!`;
}

/*
 Duplicate {object}
 */
function buildDuplicate (sourceUid, { internalId, rules, sessionName, source }) {
  assert.strictEqual(typeof sourceUid, 'string', 'Expect <sourceUid> to be a {string}');

  return { sourceUid, internalId, rules, sessionName, source };
}

function buildDuplicateFromDocObject (docObject) {
  return buildDuplicate(
    get(docObject, SOURCE_UID_PATH),
    {
      internalId: get(docObject, INTERNAL_ID_PATH),
      source: get(docObject, SOURCE_PATH),
      sessionName: get(docObject, SESSION_NAME),
    },
  );
}
