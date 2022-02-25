'use strict';

const es = require('elasticsearch');
const _ = require('lodash');
const debug = require('debug')('co-deduplicate');

const Promise = require('bluebird');
const generate = require('nanoid/generate');
const fse = require('fs-extra');
const path = require('path');
const esConf = require('co-config/es.js');
const esMapping = require('co-config/mapping.json');

const scenario = require('co-config/scenario.json');
const rules = require('co-config/rules_certain.json');
const baseRequest = require('co-config/base_request.json');
const providerRules = require('co-config/rules_provider.json');
const metadata = require('co-config/metadata-xpaths.json');
const idAlphabet = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_';

const scriptList = loadPainlessScripts();

const esClient = new es.Client({
  host: esConf.host,
  log: {
    type: 'file',
    level: ['error'],
  },
});

const business = {};

business.beforeAnyJob = function (cbBefore) {
  const options = {
    processLogs: [],
    errLogs: [],
  };

  const conditorSession = process.env.CONDITOR_SESSION || esConf.index;
  createIndex(conditorSession, options, function (err) {
    options.errLogs.push('callback createIndex, err=' + err);
    return cbBefore(err, options);
  });
};

business.doTheJob = function (docObject, cb) {
  let error;

  getByIdSource(docObject)
    .then(erase.bind(this, docObject))
    .then(existNotice.bind(this, docObject))
    .then(function (result) {
      if (result && result._id && !docObject.idElasticsearch) { docObject.idElasticsearch = result._id; }
      return cb();
    }).catch(function (e) {
      error = {
        errCode: 3,
        errMessage: 'erreur de dédoublonnage : ' + e,
      };
      docObject.error = error;
      cb(error);
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

function insereNotice (docObject) {
  return Promise.try(() => {
    const options = { index: esConf.index, type: esConf.type, refresh: true };

    debug(esConf);

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

    return esClient.index(options);
  });
}

function aggregeNotice (docObject, data) {
  return Promise.try(() => {
    const duplicates = [];
    let allMergedRules = [];
    let idchain = [];
    let arrayIdConditor = [];
    const regexp = /.*:(.*)/g;

    _.each(data.hits.hits, (hit) => {
      if (hit._source.idConditor !== docObject.idConditor) {
        duplicates.push({ rules: hit.matched_queries, source: hit._source.source, sessionName: hit._source.sessionName, idConditor: hit._source.idConditor, sourceUid: hit._source.sourceUid });
        idchain = _.union(idchain, hit._source.idChain.split('!'));
        allMergedRules = _.union(hit.matched_queries, allMergedRules);
      }
    });

    _.compact(idchain);

    arrayIdConditor = _.map(idchain, (idConditor) => {
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

    const options = { index: esConf.index, type: esConf.type, refresh: true };

    debug(esConf);

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
  });
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
  let option;
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

  options = { update: { _index: esConf.index, _type: esConf.type, _id: docObject.idElasticsearch, retry_on_conflict: 3 } };
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

  option = { body: body };

  return esClient.bulk(option);
}

function getDuplicateByIdConditor (docObject, data, result) {
  docObject.idElasticsearch = result._id;
  const request = _.cloneDeep(baseRequest);
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

function dispatch (docObject, data) {
  return Promise.try(() => {
    // creation de l'id
    if (docObject.idConditor === undefined) { docObject.idConditor = generate(idAlphabet, 25); }

    if (data.hits.total === 0) {
      return insereNotice(docObject).catch(function (err) {
        if (err) { throw new Error('Erreur d insertion de notice: ' + err); }
      });
    } else {
      return aggregeNotice(docObject, data)
        .then(getDuplicateByIdConditor.bind(null, docObject, data))
        .then(propagate.bind(null, docObject, data))
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
function existNotice (docObject) {
  return Promise.try(() => {
    let request = _.cloneDeep(baseRequest);
    let data;
    // construction des règles par scénarii
    request = buildQuery(docObject, request);
    if (request.query.bool.should.length === 0) {
      docObject.isDeduplicable = false;
      data = { hits: { total: 0 } };
      return dispatch(docObject, data);
    } else {
      docObject.isDeduplicable = true;
      return esClient.search({
        index: esConf.index,
        body: request,
      }).then(dispatch.bind(null, docObject));
    }
  });
}

function deleteNotice (docObject, data) {
  docObject.idConditor = data.hits.hits[0]._source.idConditor;
  return esClient.delete({
    index: esConf.index,
    type: esConf.type,
    id: data.hits.hits[0]._id,
    refresh: true,
  });
}

function getDuplicateByIdChain (docObject, data, result) {
  if (data.hits.hits[0]._source.isDuplicate) {
    const request = _.cloneDeep(baseRequest);
    request.query.bool.should.push({ bool: { must: [{ match: { idChain: data.hits.hits[0]._source.idChain } }] } });
    request.query.bool.minimum_should_match = 1;
    return esClient.search({
      index: esConf.index,
      body: request,
    });
  } else {
    const answer = { hits: { total: 0 } };

    return Promise.try(() => {
      return answer;
    });
  }
}

function propagateDelete (docObject, data, result) {
  let options;
  let update;
  const body = [];
  if (result.hits.total > 0) {
    _.each(result.hits.hits, (hit) => {
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

    return esClient.bulk({ body: body });
  }
}

function erase (docObject, data) {
  return Promise.try(() => {
    if (data.hits.total >= 2) {
      throw new Error('Erreur de mise à jour de notice : ID source présent en plusieurs exemplaires');
    } else if (data.hits.total === 1) {
      return deleteNotice(docObject, data)
        .then(getDuplicateByIdChain.bind(null, docObject, data))
        .then(propagateDelete.bind(null, docObject, data))
        .catch(function (e) {
          throw new Error('Erreur de mise à jour de notice : ' + e);
        });
    }
  });
}

function getByIdSource (docObject) {
  const request = _.cloneDeep(baseRequest);
  let requestSource;
  let data;

  _.each(providerRules, (providerRule) => {
    if (docObject.source.trim() === providerRule.source.trim() && testParameter(docObject, providerRule)) {
      request.query.bool.should.push(interprete(docObject, providerRule, ''));
      requestSource = {
        bool: {
          must: [
            { term: { source: docObject.source.trim() } },
          ],
          _name: 'provider',
        },
      };

      request.query.bool.should.push(requestSource);
      request.query.bool.minimum_should_match = 2;
    }
  });

  if (request.query.bool.should.length === 0) {
    data = { hits: { total: 0 } };

    return Promise.try(() => {
      return data;
    });
  } else {
    return esClient.search({
      index: esConf.index,
      body: request,
    });
  }
}

// Fonction d'ajout de l'alias si nécessaire
function createAlias (aliasArgs, options, aliasCallback) {
  let error;

  // Vérification de l'existance de l'alias, création si nécessaire, ajout de l'index nouvellement créé à l'alias
  esClient.indices.existsAlias(aliasArgs, function (err, response, status) {
    if (err) console.log(err);
    if (status !== '200') {
      esClient.indices.putAlias(aliasArgs, function (err, response, status) {
        if (!err) {
          options.processLogs.push('Création d\'un nouvel alias OK. Status : ' + status + '\n');
        } else {
          options.errLogs.push('Erreur création d\'alias. Status : ' + status + '\n');
          error = {
            errCode: 1703,
            errMessage: 'Erreur lors de la création de l\'alias : ' + err,
          };
        }
        aliasCallback(error);
      });
    } else {
      esClient.indices.updateAliases({
        actions: [{
          add: aliasArgs,
        }],

      }, function (err, response, status) {
        if (!err) {
          options.processLogs.push('Update d\'alias OK. Status : ' + status + '\n');
        } else {
          options.errLogs.push('Erreur update d\'alias. Status : ' + status + '\n');
          error = {
            errCode: 1704,
            errMessage: 'Erreur lors de la création de l\'alias : ' + err,
          };
        }
        aliasCallback(error);
      });
    }
  });
}

// fonction préalable de création d'index si celui-ci absent.
// appelé dans beforeAnyJob

function createIndex (conditorSession, options, indexCallback) {
  const reqParams = {
    index: conditorSession,
  };

  let mappingExists = true;
  let error;

  esClient.indices.exists(reqParams, function (err, response, status) {
    if (err) console.log(err);
    if (status !== 200) {
      options.processLogs.push('... Mapping et index introuvables, on les créé\n');
      mappingExists = false;
    } else {
      options.processLogs.push('... Mapping et index déjà existants\n');
    }

    if (!mappingExists) {
      esMapping.settings.index = {
        number_of_replicas: 0,
      };

      reqParams.body = esMapping;

      esClient.indices.create(reqParams, function (err, response, status) {
        if (status !== 200) {
          options.errLogs.push('... Erreur lors de la création de l\'index :\n' + err);
          error = {
            errCode: '001',
            errMessage: 'Erreur lors de la création de l\'index : ' + err,
          };
          return indexCallback(error);
        }

        createAlias({
          index: esConf.index,
          name: 'integration',
          body: { actions: { add: { index: esConf.index, alias: 'integration' } } },
        }, options, function (err) {
          indexCallback(err);
        });
      });
    } else {
      indexCallback();
    }
  });
}

function loadPainlessScripts () {
  const slist = {};
  const scriptDir = path.join(__dirname, 'painless');
  const scriptFiles = fse.readdirSync(scriptDir);
  for (const scriptFileName of scriptFiles) {
    if (scriptFileName.endsWith('.painless')) {
      const scriptName = scriptFileName.replace('.painless', '');
      const scriptPath = path.join(__dirname, 'painless', scriptFileName);
      const scriptContent = fse.readFileSync(scriptPath, { encoding: 'utf8' }).replace(/\r?\n|\r/g, '').trim();
      slist[scriptName] = scriptContent;
    }
  }
  return slist;
}
