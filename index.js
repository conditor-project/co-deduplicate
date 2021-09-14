'use strict';

const es = require('elasticsearch');
const _ = require('lodash');
const debug = require('debug')('co-deduplicate');

const generate = require('nanoid/generate');
const fse = require('fs-extra');
const path = require('path');
const esConf = require('co-config/es.js');
let esMapping = require('co-config/mapping.json');

const scenario = require('co-config/scenario.json');
const rules = require('co-config/rules_certain.json');
const providerRules = require('co-config/rules_provider.json');
const metadata = require('co-config/metadata-xpaths.json');
const idAlphabet = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_';

const scriptList = loadPainlessScripts();

const esClient = new es.Client({
  host: esConf.host,
  httpAuth:esConf.httpAuth,
  log: {
    type: 'file',
    level: ['error']
  }
});

const business = {};

/**
 * Create Elasticsearch index if needed (index name and mapping are taken from co-config)
 * @param {*} cbBefore 
 */
business.beforeAnyJob = function (cbBefore) {
  let options = {
    processLogs: [],
    errLogs: []
  };

  let conditorSession = process.env.CONDITOR_SESSION || esConf.index;
  createIndex(conditorSession, options, function (err) {
    options.errLogs.push('callback createIndex, err=' + err);
    return cbBefore(err, options);
  });
};

/**
 * Chain all deduplication steps (search in ES database & ES index update).
 * Main steps :
 * 1) getByIdSource(): check if record is already present (record identified by sourceName/sourceId pair)
 * 2) erase(): remove it if already present (it will be re-create)
 * 3) existNotice(): add record (with duplicates) in ES, and update each duplicate records in ES
 * 4) final callback: set idElasticsearch in docObject
 * 
 * @param {*} docObject 
 * @param {*} cb 
 */
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
        errMessage: 'erreur de dédoublonnage : ' + e
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

/**
 * Build JSON record (metadata + options + ids + init empty fields)
 * from docObject and index it in Elasticsearch
 * @param {*} docObject 
 * @returns 
 */
function insereNotice (docObject) {
  return promiseTry(() => {
    let options = { index: esConf.index, refresh: "true" };

    debug(esConf);

    options.body = {
      'creationDate': new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '')
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

/**
 * Build "consolidated" JSON record.
 * (The same as "insereNotice()", with duplication info : idChain, isDuplicate, duplicates and duplicateRules)
 * from docObject and index it in Elasticsearch
 * @param {*} docObject 
 * @param {*} data ES hits containing duplicates of docObject
 * @returns 
 */
 function aggregeNotice (docObject, data) {
  return promiseTry(() => {
    let duplicates = [];
    let allMergedRules = [];
    let idchain = [];
    let arrayIdConditor = [];
    let regexp = new RegExp('.*:(.*)', 'g');

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

    let options = { index: esConf.index, refresh: "true" };

    debug(esConf);

    options.body = {
      'creationDate': new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '')
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

/**
 * Update duplication info for each duplicates record
 * @param {*} docObject 
 * @param {*} data 
 * @param {*} result ES hits containing Duplicates (1 hit = 1 duplicate)
 * @returns 
 */
function propagate (docObject, result) {
  let options;
  let update;
  let body = [];
  let option;
  let matchedQueries;

  // By default, create a link between all duplicates
  // (for each record of results, init duplicates[] with idConditor, sourcesUid, sessionName and source)
  _.each(result.hits.hits, (hitTarget) => {
    options = { update: { _index: esConf.index, _id: hitTarget._id }, retry_on_conflict: 3 };
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
                source: hitSource._source.source
              }],
              idConditor: hitSource._source.idConditor
            } 
          }
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

    options = { update: { _index: esConf.index, _id: hit._id, retry_on_conflict: 3 } };

    update = {
      script:
      {
        lang: 'painless',
        source: scriptList.removeDuplicate,
        params: { idConditor: docObject.idConditor }
      }
     
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
              source: docObject.source
            }]
          }
        }
        
      };
      body.push(options);
      body.push(update);
    }

    update = {
      script:
      {
        lang: 'painless',
        source: scriptList.setIdChain,
        params: { idChain: docObject.idChain }
      }
      
    };

    body.push(options);
    body.push(update);

    update = {
      script:
      {
        lang: 'painless',
        source: scriptList.setIsDuplicate
      }
      
    };

    body.push(options);
    body.push(update);

    update = {
      script:
      {
        lang: 'painless',
        source: scriptList.setDuplicateRules
      }
      
    };

    body.push(options);
    body.push(update);

    update = {
      script:
      {
        lang: 'painless',
        source: scriptList.setHasTransDuplicate
      }
      
    };

    body.push(options);
    body.push(update);
  });

  options = { update: { _index: esConf.index, _id: docObject.idElasticsearch, retry_on_conflict: 3 } };
  update = {
    script:
    {
      lang: 'painless',
      source: scriptList.setHasTransDuplicate
    }
  };

  body.push(options);
  body.push(update);

  option = { body: body, refresh: "true" };
  return esClient.bulk(option);
}

/**
 * Get full records of Duplicates by querying Elastic (getDuplicatesByConditorIds())
 * Note : list of idConditor must have been set in `docObject.arrayIdConditor`
 * @param {*} docObject 
 * @param {*} data ES hits containing duplicates of docObject
 * @param {*} result 
 * @returns 
 */
function getDuplicatesByConditorIds (docObject, result) {
  docObject.idElasticsearch = result._id;
  let request = getBaseRequest();
  _.each(docObject.arrayIdConditor, (idConditor) => {
    if (idConditor.trim() !== '') {
      request.query.bool.should.push({ 'bool': { 'must': [{ 'term': { 'idConditor': idConditor } }] } });
    }
  });

  _.unset(docObject, 'arrayIdConditor');

  request.query.bool.minimum_should_match = 1;

  return esClient.search({
    index: esConf.index,
    body: request
  });
}

/**
 * Add or update record(s) information, with duplicates info if needed.
 * Job done in several steps :
 * 1a) if no duplicates previously found :just index docObject record and return
 * 1b) if duplicates found : build full jsonRecord from docObject and index it (agregeNotice())
 * 2) get full records of Duplicates by querying Elastic (getDuplicatesByConditorIds())
 *    (INCLUDING DOCOBJECT RECORD ITSELF !!!)
 * 3) update duplication info for each duplicate record (propagate()) 
 * @param {*} docObject 
 * @param {*} data ES Results of duplicate search
 * @returns 
 */
function dispatch (docObject, data) {
  return promiseTry(() => {
    // creation de l'id
    if (docObject.idConditor === undefined) { docObject.idConditor = generate(idAlphabet, 25); }

    if (data.hits.total.value === 0) {
      return insereNotice(docObject).catch(function (err) {
        if (err) { throw new Error('Erreur d insertion de notice: ' + err); }
      });
    } else {
      return aggregeNotice(docObject, data)
        .then(getDuplicatesByConditorIds.bind(null, docObject))
        .then(propagate.bind(null, docObject))
        .catch((err) => {
          if (err) { throw new Error('Erreur d aggregation de notice: ' + err); }
        });
    }
  });
}

function testParameter (docObject, rules) {
  let arrayParameter = (rules.non_empty !== undefined) ? rules.non_empty : [];
  let arrayNonParameter = (rules.is_empty !== undefined) ? rules.is_empty : [];
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

/**
 * Compute a query fragment for a given rule
 * @param {*} docObject current record, from which field values will be taken
 * @param {*} rule non-interpreted rule taken from config file
 * @param {*} type document type (aka "typeConditor")
 * @returns 
 */
function interprete (docObject, rule, type) {
  let isEmpty = (rule.is_empty !== undefined) ? rule.is_empty : [];
  let query = rule.query;
  let rulename;

  if (type.trim() !== '') {
    rulename = type + ' : ' + query.bool._name;
  } else {
    rulename = query.bool._name;
  }

  const newQuery = {
    bool: {
      must: null,
      _name: rulename
    }
  };

  newQuery.bool.must = _.map(query.bool.must, (value) => {
    let term, match, bool;

    if (value.match && _.isString(_.get(docObject, _.values(value.match)[0]))) {
      match = { 'match': null };
      match.match = _.mapValues(value.match, (pattern) => {
        return _.get(docObject, pattern);
      });
      return match;
    } else if (value.term && _.isString(_.get(docObject, _.values(value.term)[0]))) {
      term = { 'term': null };
      term.term = _.mapValues(value.term, (pattern) => {
        return _.get(docObject, pattern);
      });
      return term;
    } else if (value.match && _.isArray(_.get(docObject, _.values(value.match)[0]))) {
      bool = { 'bool': {} };
      bool.bool.should = _.map(_.get(docObject, _.values(value.match)[0]), (testValue) => {
        let shouldMatch;
        shouldMatch = { 'match': {} };
        shouldMatch.match[_.keys(value.match)[0]] = testValue;
        return shouldMatch;
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    } else if (value.term && _.isArray(_.get(docObject, _.values(value.term)[0]))) {
      bool = { 'bool': {} };
      bool.bool.should = _.map(_.get(docObject, _.values(value.term)[0]), (testValue) => {
        let shouldMatch;
        shouldMatch = { 'term': {} };
        shouldMatch.match[_.keys(value.term)[0]] = testValue;
        return shouldMatch;
      });
      bool.bool.minimum_should_match = 1;
      return bool;
    } else if (value.bool) {
      bool = { 'bool': {} };
      bool.bool.should = _.map(value.bool.should, (shouldCond) => {
        let shouldTerm, shouldMatch;
        if (shouldCond.match && _.isString(_.get(docObject, _.values(shouldCond.match)[0]))) {
          shouldMatch = { 'match': null };
          shouldMatch.match = _.mapValues(shouldCond.match, (pattern) => {
            return _.get(docObject, pattern);
          });
          return shouldMatch;
        } else if (shouldCond.term && _.isString(_.get(docObject, _.values(shouldCond.term)[0]))) {
          shouldTerm = { 'term': null };
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
    newQuery.bool.must_not.push({ 'exists': { 'field': field + '.normalized' } });
  });
  if (type !== '') {
    newQuery.bool.must.push({ 'match': { 'typeConditor.normalized': type } });
  }
  return newQuery;
}

/**
 * Build full Elastic query for searching duplicates.
 * Query is composed by a list of rules.
 * All existing rules are defined in co-config/rules_certain.json
 * Rules actually used depends on sourceName. There are listed in co-config/scenario.json
 * For each rule, the query fragment is computed by calling "interprete()"
 * @param {*} docObject 
 * @param {*} request 
 * @returns 
 */
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

/**
 * Search duplicates in Elastic index.
 * First build elastic query by calling "buildQuery()", then execute it,
 * and finally update Elastic index by calling "dispatch()"
 * @param {*} docObject 
 * @returns 
 */
function existNotice (docObject) {
  return promiseTry(() => {
    let request = getBaseRequest();
    let data;
    // build rules query according to scénarii
    request = buildQuery(docObject, request);
    if (request.query.bool.should.length === 0) {
      // no rule/scenario found => no duplication possible 
      // => dispatch with fake ES resut with 0 result
      // => insert new doc without duplicates
      docObject.isDeduplicable = false;
      data = { 'hits': { 'total': { 'value':0 } } };
      return dispatch(docObject, data);
    } else {
      docObject.isDeduplicable = true;
      // execute search query and send results to dispatch() function
      return esClient.search({
        index: esConf.index,
        body: request
      }).then(dispatch.bind(null, docObject));
    }
  });
}

/**
 * Call to Elastic DELETE route, but first get old idConditor to reuse in new docObject (to avoid creation of new idConditor)
 * @param {*} docObject record which will be inserted in Elasticsearch later
 * @param {*} data record which has to be deleted
 * @returns 
 */
function deleteNotice (docObject, data) {
  docObject.idConditor = data.hits.hits[0]._source.idConditor;
  return esClient.delete({
    index: esConf.index,
    type: esConf.type,
    id: data.hits.hits[0]._id,
    refresh: "true"
  });
}

/**
 * Get all duplicates of a record, based on "idChain" identifier
 * (in order to modify each of found records later )
 * @param {*} data record which has to be deleted
 * @returns 
 */
function getDuplicatesByIdChain (data) {
  if (data.hits.hits[0]._source.isDuplicate) {
    let request = getBaseRequest();
    request.query.bool.should.push({ 'bool': { 'must': [{ 'match': { 'idChain': data.hits.hits[0]._source.idChain } }] } });
    request.query.bool.minimum_should_match = 1;
    return esClient.search({
      index: esConf.index,
      body: request
    });
  } else {
    let answer = { 'hits': { 'total': { 'value':0 } } };

    return promiseTry(() => {
      return answer;
    });
  }
}

/**
 * Propagate deletion of "docObject.idConditor" on all duplicates found by getDuplicatesByIdChain
 * Job done by 4 painless scripts
 * @param {*} docObject record which will be inserted in Elasticsearch later
 * @param {*} result duplicates previously found to update
 * @returns 
 */
function propagateDelete (docObject, result) {
  let options;
  let update;
  let body = [];
  let option;
  if (result.hits.total.value > 0) {
    _.each(result.hits.hits, (hit) => {
      options = { update: { _index: esConf.index, _id: hit._id, retry_on_conflict: 3 } };

      update = {
        script:
        {
          lang: 'painless',
          source: scriptList.removeDuplicate,
          params: { idConditor: docObject.idConditor }
        }
      };

      body.push(options);
      body.push(update);

      update = {
        script:
        {
          lang: 'painless',
          source: scriptList.setIdChain
        }
      };

      body.push(options);
      body.push(update);

      update = {
        script:
        {
          lang: 'painless',
          source: scriptList.setIsDuplicate
        }
      };

      body.push(options);
      body.push(update);

      update = {
        script:
        {
          lang: 'painless',
          source: scriptList.setDuplicateRules
        }
      };

      body.push(options);
      body.push(update);
    });

    option = { body: body, refresh: "true" };
    return esClient.bulk(option);
  }
}

/**
 * Reomve record from Elasticsearch database, with update of duplicates if needed
 * Do nothing in case of new document
 * @param {*} docObject record which will be inserted in Elasticsearch later
 * @param {*} data record to delete (Elastic result from query ran in "getByIdSource" function)
 * @returns 
 */
function erase (docObject, data) {
  return promiseTry(() => {
    if (data.hits.total.value >= 2) {
      throw new Error('Erreur de mise à jour de notice : ID source présent en plusieurs exemplaires');
    } else if (data.hits.total.value === 1) {
      return deleteNotice(docObject, data)
        .then(getDuplicatesByIdChain.bind(null, data))
        .then(propagateDelete.bind(null, docObject))
        .catch(function (e) {
          throw new Error('Erreur de mise à jour de notice : ' + e);
        });
    }
  });
}

/**
 * According to "rules_provides.json" config file, check if a document 
 * with the same sourceName/sourceId couple already exists in database.
 * Note : Config file allow to know in which field is stored sourceId
 *  
 * @param {*} docObject 
 * @returns Elastic results of search query
 */
function getByIdSource (docObject) {
  let request = getBaseRequest();
  let requestSource;
  let data;

  _.each(providerRules, (providerRule) => {
    if (docObject.source.trim() === providerRule.source.trim() && testParameter(docObject, providerRule)) {
      request.query.bool.should.push(interprete(docObject, providerRule, ''));
      requestSource = {
        'bool': {
          'must': [
            { 'term': { 'source': docObject.source.trim() } }
          ],
          '_name': 'provider'
        }
      };

      request.query.bool.should.push(requestSource);
      request.query.bool.minimum_should_match = 2;
    }
  });

  if (request.query.bool.should.length === 0) {
    data = { 'hits': { 'total': { 'value':0 } } };

    return promiseTry(() => {
      return data;
    });
  } else {
    return esClient.search({
      index: esConf.index,
      body: request
    });
  }
}

// fonction préalable de création d'index si celui-ci absent.
// appelé dans beforeAnyJob

function createIndex (conditorSession, options, indexCallback) {
  let reqParams = {
    index: conditorSession
  };

  let mappingExists = true;
  let error;

  esClient.indices.exists(reqParams, function (err, response, status) {
    if (err) console.log(err);
    if (status !== 200) {
      options.processLogs.push('... Mapping et index introuvables, on les créé\n');
      esMapping.settings.index = {
        'number_of_replicas': 0,
        'number_of_shards': 5
      };

      reqParams.body = esMapping;

      esClient.indices.create(reqParams, function (err, response, status) {
        if (status !== 200) {
          options.errLogs.push('... Erreur lors de la création de l\'index :\n' + err);
          error = {
            errCode: '001',
            errMessage: 'Erreur lors de la création de l\'index : ' + err
          };
          return indexCallback(error);
        } else {
          indexCallback();
        };
      });
    } else {
      options.processLogs.push('... Mapping et index déjà existants\n');
      indexCallback();
    }
  });
}

/**
 * Read all painless scripts in `painless` directory and return it
 * @returns an object where keys are script names, and values script contents (string form)
 */
function loadPainlessScripts () {
  const slist = {};
  const scriptDir = path.join(__dirname, 'painless');
  const scriptFiles = fse.readdirSync(scriptDir);
  for (let scriptFileName of scriptFiles) {
    if (scriptFileName.endsWith('.painless')) {
      const scriptName = scriptFileName.replace('.painless', '');
      const scriptPath = path.join(__dirname, 'painless', scriptFileName);
      const scriptContent = fse.readFileSync(scriptPath, { encoding: 'utf8' }).replace(/\r?\n|\r/g, '').trim();
      slist[scriptName] = scriptContent;
    }
  }
  return slist;
}


function getBaseRequest() {
  return {
    "query": {
      "bool": {
        "should": [

        ],
        "minimum_should_match": 1
      }
    }
  }
}

function promiseTry(func) {
  return new Promise(function(resolve, reject) {
      resolve(func());
  })
}
