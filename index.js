const { has, pick, _ } = require('lodash');
const fs = require('fs-extra');
const path = require('path');

const { deduplicate: { scenario, rules } } = require('corhal-config');

const scriptList = loadPainlessScripts();

const { elastic: { aliases, indices } } = require('@istex/config-component').get(module);
const { getBaseRequest } = require('./src/getBaseRequest');
const esClient = require('./helpers/esHelpers/client').get();
const { search, bulk, update, aggregate, updateDuplicatesTree } = require('./src/documentsManager');
const EventEmitter = require('events');

class Business extends EventEmitter {
  doTheJob (docObject, cb) {
    deduplicate(docObject)
      .then(() => cb())
      .catch(cb);
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

      const request = buildQuery(docObject);

      if (request.query.bool.should.length === 0) {
        business.emit('info', `Not deduplicable {docObject}, internalId: ${docObject.technical.internalId}`);
        docObject.business.isDeduplicable = false;
        return update(aliases.TO_DEDUPLICATE,
          docObject.technical.internalId,
          { doc: { business: { isDeduplicable: false } } });
      }

      docObject.business.isDeduplicable = true;

      return search({
        index: aliases.TO_DEDUPLICATE,
        body: request,
        size: 1000, // This means, 1000 duplicates found max.
      }).then((result) => {
        const { body: { hits } } = result;
        if (hits.total.value === 0) {
          business.emit('info',
            `No duplicates found for {docObject}, internalId: ${docObject.technical.internalId}`);

          docObject.business.isDuplicate = false;

          return update(indices.documents.index,
            docObject.technical.internalId,
            { doc: pick(docObject, ['business.isDeduplicable', 'business.isDuplicate']) });
        }
        return updateDuplicatesTree(docObject, hits.hits);
         //return dispatchDuplicatesTree(docObject, result);
      });
    },
  );
}

function buildQuery (docObject) {
  if (!has(docObject, 'business.duplicateGenre')) {
    throw new Error(`Expected Object ${docObject.technical.internalId} to have property business.duplicateGenre`);
  }

  const request = getBaseRequest();

  if (scenario[docObject.business.duplicateGenre]) {
    _.each(rules, (rule) => {
      if (_.includes(scenario[docObject.business.duplicateGenre], rule.rule) &&
          validateRequiredAndForbiddenParameters(docObject, rule)
      ) {
        request.query.bool.should.push(buildQueryFromRule(docObject, rule, docObject.business.duplicateGenre));
      }
    });
  }

  request.query.bool.must_not = { term: { _id: docObject.technical.internalId } };

  return request;
}

function dispatchDuplicatesTree (docObject, result) {
  return Promise.resolve().then(() => {
    const { body: { hits: { hits = [] } } } = result;

    return aggregate(docObject, hits)
      .then(() => getDuplicatesBySourceUid(docObject))
      .then((result) => propagate(docObject, result));
  });
}

function getDuplicatesBySourceUid (docObject) {
  const request = getBaseRequest();
  _.each(docObject.business.duplicates, (duplicate) => {
    request.query.bool.should.push({ bool: { must: [{ term: { sourceUid: duplicate.sourceUid } }] } });
  });

  request.query.bool.minimum_should_match = 1;

  return search({
    index: indices.documents.index,
    body: request,
  });
}

function propagate (docObject, result) {
  let options;
  let update;
  const body = [];
  let matchedQueries;

  // On crée une liaison par défaut entre tous les duplicats trouvés
  _.each(result.hits.hits, (targetDocument) => {
    options = { update: { _index: indices.documents.index, _id: targetDocument._id }, retry_on_conflict: 3 };
    _.each(result.hits.hits, (duplicateDocument) => {
      update = {
        script:
            {
              lang: 'painless',
              source: scriptList.addEmptyDuplicate,
              params: {
                duplicates: [{
                  internalId: duplicateDocument._source.technical.internalId,
                  sourceUid: duplicateDocument._source.sourceUid,
                  rules: [],
                  sessionName: duplicateDocument._source.technical.sessionName,
                  source: duplicateDocument._source.source,
                }],
                idConditor: hitSource._source.idConditor,
              },
            },
        refresh: true,

      };

      body.push(options);
      body.push(update);
    });
  });

  _.each(result.hits.hits, (hit) => {
    matchedQueries = [];

    _.each(docObject.duplicates, (directDuplicate) => {
      if (directDuplicate.idConditor === hit._source.idConditor) { matchedQueries = directDuplicate.rules; }
    });

    options = { update: { _index: esConf.index, _type: esConf.type, _id: hit._id, retry_on_conflict: 3 } };

    update = {
      script:
        {
          lang: 'painless',
          source: scriptList.removeDuplicate,
          params: { idConditor: docObject.idConditor },
        },
      refresh: true,
    };

    body.push(options);
    body.push(update);

    if (hit._source.idConditor !== docObject.idConditor) {
      update = {
        script:
          {
            lang: 'painless',
            source: scriptList.addDuplicate,
            params: {
              duplicates: [{
                idConditor: docObject.idConditor,
                sourceUid: docObject.sourceUid,
                rules: matchedQueries,
                sessionName: docObject.sessionName,
                source: docObject.source,
              }],
            },
          },
        refresh: true,
      };
      body.push(options);
      body.push(update);
    }

    update = {
      script:
        {
          lang: 'painless',
          source: scriptList.setIdChain,
          params: { idChain: docObject.idChain },
        },
      refresh: true,
    };

    body.push(options);
    body.push(update);

    update = {
      script:
        {
          lang: 'painless',
          source: scriptList.setIsDuplicate,
        },
      refresh: true,
    };

    body.push(options);
    body.push(update);

    update = {
      script:
        {
          lang: 'painless',
          source: scriptList.setDuplicateRules,
        },
      refresh: true,
    };

    body.push(options);
    body.push(update);

    update = {
      script:
        {
          lang: 'painless',
          source: scriptList.setHasTransDuplicate,
        },
      refresh: true,
    };

    body.push(options);
    body.push(update);
  });

  options = {
    update: {
      _index: esConf.index,
      _type: esConf.type,
      _id: docObject.idElasticsearch,
      retry_on_conflict: 3,
    },
  };
  update = {
    script:
      {
        lang: 'painless',
        source: scriptList.setHasTransDuplicate,
      },
    refresh: true,
  };

  body.push(options);
  body.push(update);

  return esClient.bulk({ body: body });
}

function validateRequiredAndForbiddenParameters (docObject, rules) {
  let isAllParametersValid = true;
  _.each(rules.non_empty, function (requiredParameter) {
    if (_.get(docObject, requiredParameter) == null ||
        (_.isArray(_.get(docObject, requiredParameter)) && _.get(docObject, requiredParameter).length === 0) ||
        (_.isString(_.get(docObject, requiredParameter)) && _.get(docObject, requiredParameter)
          .trim() === '')) { isAllParametersValid = false; }
  });

  _.each(rules.is_empty, function (forbiddenParameter) {
    if (_.get(docObject, forbiddenParameter) != null &&
        ((_.isArray(_.get(docObject, forbiddenParameter)) && _.get(docObject, forbiddenParameter).length > 0) ||
         (_.isString(_.get(docObject, forbiddenParameter)) && _.get(docObject, forbiddenParameter).trim() !== '')
        )) { isAllParametersValid = false; }
  });

  return isAllParametersValid;
}

function buildQueryFromRule (docObject, rule, type = null) {
  let rulename;

  if (_.isString(type) && type.trim().length > 0) {
    rulename = type + ' : ' + rule.query.bool._name;
  } else {
    rulename = rule.query.bool._name;
  }

  const newQuery = {
    bool: {
      _name: rulename,
    },
  };

  newQuery.bool.must = _.map(rule.query.bool.must, (must) => {
    const match = {};
    const term = {};
    const bool = { bool: {} };

    if (must.match && _.isString(_.get(docObject, _.values(must.match)[0]))) {
      match.match = _.mapValues(must.match, (fieldPath) => {
        return _.get(docObject, fieldPath);
      });
      return match;
    }

    if (must.term && _.isString(_.get(docObject, _.values(must.term)[0]))) {
      term.term = _.mapValues(must.term, (pattern) => {
        return _.get(docObject, pattern);
      });
      return term;
    }

    if (must.match && _.isArray(_.get(docObject, _.values(must.match)[0]))) {
      bool.bool.should = _.map(_.get(docObject, _.values(must.match)[0]), (testValue) => {
        const shouldMatch = { match: {} };
        shouldMatch.match[_.keys(must.match)[0]] = testValue;
        return shouldMatch;
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    }

    if (must.term && _.isArray(_.get(docObject, _.values(must.term)[0]))) {
      bool.bool.should = _.map(_.get(docObject, _.values(must.term)[0]), (testValue) => {
        const shouldMatch = { term: {} };
        shouldMatch.match[_.keys(must.term)[0]] = testValue;
        return shouldMatch;
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    }

    if (must.bool) {
      bool.bool.should = _.map(must.bool.should, (should) => {
        const shouldTerm = {};
        const shouldMatch = {};

        if (should.match && _.isString(_.get(docObject, _.values(should.match)[0]))) {
          shouldMatch.match = _.mapValues(should.match, (fieldPath) => {
            return _.get(docObject, fieldPath);
          });
          return shouldMatch;
        }

        if (should.term && _.isString(_.get(docObject, _.values(should.term)[0]))) {
          shouldTerm.term = _.mapValues(should.term, (fieldPath) => {
            return _.get(docObject, fieldPath);
          });
          return shouldTerm;
        }
      });

      bool.bool.minimum_should_match = 1;
      return bool;
    }
  });

  if (_.isString(type) && type.trim().length > 0) {
    newQuery.bool.must.push({ match: { 'business.duplicateGenre': type } });
  }

  if (_.get(rule, 'is_empty.length') > 0) {
    newQuery.bool.must_not = [];
    _.each(rule.isEmpty, (field) => {
      newQuery.bool.must_not.push({ exists: { field: field } });
    });
  }
  return newQuery;
}

function propagateDelete (docObject, { body: { hits } }) {
  let options;
  let update;
  const body = [];
  if (hits.total.value > 0) {
    _.each(hits.hits, (hit) => {
      options = { update: { _index: aliases.TO_DEDUPLICATE, _id: hit._id, retry_on_conflict: 3 } };

      update = {
        script:
          {
            lang: 'painless',
            source: scriptList.removeDuplicate,
            params: { idConditor: docObject.idConditor },
          },
        refresh: true,
      };

      body.push(options);
      body.push(update);

      update = {
        script:
          {
            lang: 'painless',
            source: scriptList.setIdChain,
          },
        refresh: true,
      };

      body.push(options);
      body.push(update);

      update = {
        script:
          {
            lang: 'painless',
            source: scriptList.setIsDuplicate,
          },
        refresh: true,
      };

      body.push(options);
      body.push(update);

      update = {
        script:
          {
            lang: 'painless',
            source: scriptList.setDuplicateRules,
          },
        refresh: true,
      };

      body.push(options);
      body.push(update);
    });

    return bulk({ body });
  }
}

function loadPainlessScripts () {
  const slist = {};
  const scriptDir = path.join(__dirname, 'painless');
  const scriptFiles = fs.readdirSync(scriptDir);
  for (const scriptFileName of scriptFiles) {
    if (scriptFileName.endsWith('.painless')) {
      const scriptName = scriptFileName.replace('.painless', '');
      const scriptPath = path.join(__dirname, 'painless', scriptFileName);
      slist[scriptName] = fs.readFileSync(scriptPath, { encoding: 'utf8' }).replace(/\r?\n|\r/g, '').trim();
    }
  }
  return slist;
}
