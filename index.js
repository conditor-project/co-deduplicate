const _ = require('lodash');
const generate = require('nanoid/generate');
const fse = require('fs-extra');
const path = require('path');

const { deduplicate: { scenario, rules, providerRules } } = require('corhal-config');
const metadata = require('co-config/metadata-xpaths.json');
const idAlphabet = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_';

const scriptList = loadPainlessScripts();

const { elastic: { aliases, indices } } = require('@istex/config-component').get(module);
const { getBaseRequest } = require('./src/getBaseRequest');
const esClient = require('./helpers/esHelpers/client').get();
const { search, deleteById, index, bulk } = require('./src/documentsManager');
const business = {};
const FAKE_EMPTY_RESULT = { body: { hits: { total: { value: 0 } } } };

business.doTheJob = function (docObject, cb) {
  getByIdSource(docObject)
    .then(result => erase(docObject, result))
    .then(() => deduplicate(docObject))
    .then(function ({ body }) {
      if (body?._id && !docObject.idElasticsearch) { docObject.idElasticsearch = body._id; }
      return cb();
    }).catch(function (reason) {
      docObject.error = {
        errCode: 3,
        errMessage: 'erreur de dédoublonnage: ' + reason,
      };

      cb(reason);
    });
};

module.exports = business;

function insertMetadata (docObject, options) {
  _.each(metadata, (metadatum) => {
    if (metadatum.indexed === undefined || metadatum.indexed === true) {
      if (_.isArray(docObject[metadatum.name])) {
        options.body[metadatum.name] = docObject[metadatum.name];
      } else if (_.isBoolean(docObject[metadatum.name]) || !_.isEmpty(docObject[metadatum.name])) {
        options.body[metadatum.name] = docObject[metadatum.name];
      }
    }
  });
}

function insertNotice (docObject) {
  const options = { refresh: true };

  options.body = {
    creationDate: new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''),
  };

  insertMetadata(docObject, options);
  insertCommonOptions(docObject, options);

  options.body.idChain = docObject.source + ':' + docObject.idConditor + '!';
  docObject.duplicates = [];
  docObject.isDuplicate = false;
  options.body.duplicates = docObject.duplicates;
  options.body.isDuplicate = docObject.isDuplicate;

  return index(indices.documents.index, options);
}

function aggregeNotice (docObject, result) {
  const { body: { hits } } = result;
  const duplicates = [];
  let allMergedRules = [];
  let idchain = [];
  const regexp = /.*:(.*)/g;

  _.each(hits.hits, (hit) => {
    if (hit._source.idConditor !== docObject.idConditor) {
      duplicates.push({
        rules: hit.matched_queries,
        source: hit._source.source,
        sessionName: hit._source.sessionName,
        idConditor: hit._source.idConditor,
        sourceUid: hit._source.sourceUid,
      });
      idchain = _.union(idchain, hit._source.idChain.split('!'));
      allMergedRules = _.union(hit.matched_queries, allMergedRules);
    }
  });

  _.compact(idchain);

  const arrayIdConditor = _.map(idchain, (idConditor) => {
    return idConditor.replace(regexp, '$1');
  });

  idchain = _.map(idchain, (idConditor) => {
    return idConditor + '!';
  });

  idchain.push(docObject.source + ':' + docObject.idConditor + '!');
  idchain.sort();

  docObject.duplicates = duplicates;
  docObject.duplicateRules = _.sortBy(allMergedRules);
  docObject.isDuplicate = (allMergedRules.length > 0);

  const options = { index: esConf.index, refresh: true };

  options.body = {
    creationDate: new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''),
  };

  insertMetadata(docObject, options);
  insertCommonOptions(docObject, options);

  options.body.duplicates = duplicates;
  options.body.duplicateRules = allMergedRules;
  options.body.isDuplicate = (allMergedRules.length > 0);
  docObject.arrayIdConditor = arrayIdConditor;
  options.body.idChain = _.join(idchain, '');
  docObject.idChain = options.body.idChain;

  return esClient.index(options);
}

function insertCommonOptions (docObject, options) {
  options.body.path = docObject.path;
  options.body.source = docObject.source;
  options.body.typeConditor = docObject.typeConditor;
  options.body.idConditor = docObject.idConditor;
  options.body.sourceId = docObject.sourceId;
  options.body.sourceUid = docObject.sourceUid;
  options.body.sessionName = docObject.sessionName;
  options.body.ingestBaseName = docObject.ingestBaseName;
  options.body.isDeduplicable = docObject.isDeduplicable;
}

function propagate (docObject, data, result) {
  let options;
  let update;
  const body = [];
  let matchedQueries;

  // On crée une liaison par défaut entre tous les duplicats trouvés
  _.each(result.hits.hits, (hitTarget) => {
    options = { update: { _index: esConf.index, _type: esConf.type, _id: hitTarget._id }, retry_on_conflict: 3 };
    _.each(result.hits.hits, (hitSource) => {
      if (hitTarget._source.idConditor !== hitSource._source.idConditor) {
        update = {
          script:
            {
              lang: 'painless',
              source: scriptList.addEmptyDuplicate,
              params: {
                duplicates: [{
                  idConditor: hitSource._source.idConditor,
                  sourceUid: hitSource._source.sourceUid,
                  rules: [],
                  sessionName: hitSource._source.sessionName,
                  source: hitSource._source.source,
                }],
                idConditor: hitSource._source.idConditor,
              },
            },
          refresh: true,

        };

        body.push(options);
        body.push(update);
      }
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

function getDuplicateByIdConditor (docObject, data, result) {
  docObject.idElasticsearch = result._id;
  const request = getBaseRequest();
  _.each(docObject.arrayIdConditor, (idConditor) => {
    if (idConditor.trim() !== '') {
      request.query.bool.should.push({ bool: { must: [{ term: { idConditor: idConditor } }] } });
    }
  });

  _.unset(docObject, 'arrayIdConditor');

  request.query.bool.minimum_should_match = 1;

  return esClient.search({
    index: esConf.index,
    body: request,
  });
}

function dispatch (docObject, result) {
  return Promise.resolve().then(() => {
    const { body: { hits } } = result;
    // creation de l'id
    if (docObject.idConditor == null) { docObject.idConditor = generate(idAlphabet, 25); }

    if (hits.total.value === 0) {
      return insertNotice(docObject).catch(function (err) {
        if (err) { throw new Error('Erreur d insertion de notice: ' + err); }
      });
    } else {
      return aggregeNotice(docObject, result)
        .then(getDuplicateByIdConditor.bind(null, docObject, result))
        .then(propagate.bind(null, docObject, result))
        .catch((err) => {
          if (err) { throw new Error('Erreur d aggregation de notice: ' + err); }
        });
    }
  });
}

function testParameter (docObject, rules) {
  const arrayParameter = (rules.non_empty !== undefined) ? rules.non_empty : [];
  const arrayNonParameter = (rules.is_empty !== undefined) ? rules.is_empty : [];
  let bool = true;
  _.each(arrayParameter, function (parameter) {
    if (_.get(docObject, parameter) === undefined ||
        (_.isArray(_.get(docObject, parameter)) && _.get(docObject, parameter).length === 0) ||
        (_.isString(_.get(docObject, parameter)) && _.get(docObject, parameter).trim() === '')) { bool = false; }
  });

  _.each(arrayNonParameter, function (nonparameter) {
    if (_.get(docObject, nonparameter) !== undefined &&
        ((_.isArray(_.get(docObject, nonparameter)) && _.get(docObject, nonparameter).length > 0) ||
         (_.isString(_.get(docObject, nonparameter)) && _.get(docObject, nonparameter).trim() !== '')
        )) { bool = false; }
  });

  return bool;
}

function interprete (docObject, rule, type) {
  const isEmpty = (rule.is_empty !== undefined) ? rule.is_empty : [];
  const query = rule.query;
  let rulename;

  if (type.trim() !== '') {
    rulename = type + ' : ' + query.bool._name;
  } else {
    rulename = query.bool._name;
  }

  const newQuery = {
    bool: {
      must: null,
      _name: rulename,
    },
  };

  newQuery.bool.must = _.map(query.bool.must, (value) => {
    let term, match, bool;

    if (value.match && _.isString(_.get(docObject, _.values(value.match)[0]))) {
      match = { match: null };
      match.match = _.mapValues(value.match, (pattern) => {
        return _.get(docObject, pattern);
      });
      return match;
    } else if (value.term && _.isString(_.get(docObject, _.values(value.term)[0]))) {
      term = { term: null };
      term.term = _.mapValues(value.term, (pattern) => {
        return _.get(docObject, pattern);
      });
      return term;
    } else if (value.match && _.isArray(_.get(docObject, _.values(value.match)[0]))) {
      bool = { bool: {} };
      bool.bool.should = _.map(_.get(docObject, _.values(value.match)[0]), (testValue) => {
        const shouldMatch = { match: {} };
        shouldMatch.match[_.keys(value.match)[0]] = testValue;
        return shouldMatch;
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    } else if (value.term && _.isArray(_.get(docObject, _.values(value.term)[0]))) {
      bool = { bool: {} };
      bool.bool.should = _.map(_.get(docObject, _.values(value.term)[0]), (testValue) => {
        const shouldMatch = { term: {} };
        shouldMatch.match[_.keys(value.term)[0]] = testValue;
        return shouldMatch;
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    } else if (value.bool) {
      bool = { bool: {} };
      bool.bool.should = _.map(value.bool.should, (shouldCond) => {
        let shouldTerm, shouldMatch;
        if (shouldCond.match && _.isString(_.get(docObject, _.values(shouldCond.match)[0]))) {
          shouldMatch = { match: null };
          shouldMatch.match = _.mapValues(shouldCond.match, (pattern) => {
            return _.get(docObject, pattern);
          });
          return shouldMatch;
        } else if (shouldCond.term && _.isString(_.get(docObject, _.values(shouldCond.term)[0]))) {
          shouldTerm = { term: null };
          shouldTerm.term = _.mapValues(shouldCond.term, (pattern) => {
            return _.get(docObject, pattern);
          });
          return shouldTerm;
        }
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    }
  });

  // ajout de la précision que les champs doivent exister dans Elasticsearch

  if (isEmpty.length > 0) { newQuery.bool.must_not = []; }

  _.each(isEmpty, (field) => {
    newQuery.bool.must_not.push({ exists: { field: field + '.normalized' } });
  });
  if (type !== '') {
    newQuery.bool.must.push({ match: { 'typeConditor.normalized': type } });
  }
  return newQuery;
}

function buildQuery (docObject, request) {
  if (scenario[docObject.typeConditor]) {
    _.each(rules, (rule) => {
      if (_.indexOf(scenario[docObject.typeConditor], rule.rule) !== -1 && testParameter(docObject, rule)) {
        request.query.bool.should.push(interprete(docObject, rule, docObject.typeConditor));
      }
    });
  }
  return request;
}

// on crée la requete puis on teste si l'entrée existe
function deduplicate (docObject) {
  // construction des règles par scénarii
  const request = buildQuery(docObject, getBaseRequest());

  if (request.query.bool.should.length === 0) {
    docObject.isDeduplicable = false;
    return dispatch(docObject, { body: { hits: { total: { value: 0 } } } });
  } else {
    docObject.isDeduplicable = true;
    return search({
      index: aliases.TO_DEDUPLICATE,
      body: request,
    }).then((result) => dispatch(docObject, result));
  }
}

function deleteNotice (docObject, result) {
  const { body: { hits } } = result;
  docObject.idConditor = hits.hits[0]._source.idConditor;
  return deleteById(
    hits.hits[0]._id,
    hits.hits[0]._index,
    { refresh: true },
  );
}

function getDuplicateByIdChain (docObject, { body: { hits } }) {
  if (hits.hits[0]._source.isDuplicate) {
    const request = getBaseRequest();
    request.query.bool.should.push({ bool: { must: [{ match: { idChain: hits.hits[0]._source.idChain } }] } });
    request.query.bool.minimum_should_match = 1;
    return search({
      index: aliases.TO_DEDUPLICATE,
      body: request,
    });
  } else {
    return FAKE_EMPTY_RESULT;
  }
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

function erase (docObject, result) {
  const { body: { hits } } = result;
  if (hits.total.value >= 2) {
    throw new Error('Erreur de mise à jour de notice : ID source présent en plusieurs exemplaires');
  } else if (hits.total.value === 1) {
    return deleteNotice(docObject, result)
      .then(() => getDuplicateByIdChain(docObject, result))
      .then((duplicateResult) => propagateDelete(docObject, duplicateResult));
  }
}

function getByIdSource (docObject) {
  const body = getBaseRequest();

  _.each(providerRules, (providerRule) => {
    if (docObject.source.trim() === providerRule.source.trim() && testParameter(docObject, providerRule)) {
      const requestSource = {
        bool: {
          must: [
            { term: { source: docObject.source.trim() } },
          ],
          _name: 'provider',
        },
      };

      body.query.bool.should.push(requestSource);
      body.query.bool.should.push(interprete(docObject, providerRule, ''));
      body.query.bool.minimum_should_match = 2;
    }
  });
  return search({ body, indice: aliases.TO_DEDUPLICATE });
}

function loadPainlessScripts () {
  const slist = {};
  const scriptDir = path.join(__dirname, 'painless');
  const scriptFiles = fse.readdirSync(scriptDir);
  for (const scriptFileName of scriptFiles) {
    if (scriptFileName.endsWith('.painless')) {
      const scriptName = scriptFileName.replace('.painless', '');
      const scriptPath = path.join(__dirname, 'painless', scriptFileName);
      slist[scriptName] = fse.readFileSync(scriptPath, { encoding: 'utf8' }).replace(/\r?\n|\r/g, '').trim();
    }
  }
  return slist;
}
