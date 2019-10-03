'use strict';

const es = require('elasticsearch');
const _ = require('lodash');
const Promise = require('bluebird');
const esConf = require('co-config/es.js');
const scenario = require('co-config/scenario.json');
const rules = require('co-config/rules_certain.json');
const baseRequest = require('co-config/base_request.json');

const esClient = new es.Client({
  host: esConf.host,
  log: {
    type: 'file',
    level: ['error']
  }
});

const coDeduplicate = {};
coDeduplicate.doTheJob = function (docObject, next) {
  Promise.resolve().then(() => {
    const cloneBaseRequest = _.cloneDeep(baseRequest);
    const request = buildQuery(docObject, cloneBaseRequest);
    if (request.query.bool.should.length === 0) {
      docObject.isDeduplicable = false;
      docObject.idChain = `${docObject.source}:${docObject.idConditor}!`;
      docObject.duplicates = [];
      docObject.isDuplicate = false;
      return next();
    }
    docObject.isDeduplicable = true;
    esClient.search({
      index: esConf.index,
      body: request
    }).then(data => {
      let duplicates = [];
      let allMergedRules = [];
      let idchain = [];

      _.each(data.hits.hits, (hit) => {
        if (hit._source.idConditor !== docObject.idConditor) {
          duplicates.push({
            rules: hit.matched_queries,
            source: hit._source.source,
            sessionName: hit._source.sessionName,
            idConditor: hit._source.idConditor
          });
          if (hit._source.hasOwnProperty('idChain')) idchain = _.union(idchain, hit._source.idChain.split('!'));
          allMergedRules = _.union(hit.matched_queries, allMergedRules);
        }
      });

      _.compact(idchain);
      idchain = _.map(idchain, (idConditor) => idConditor + '!');
      idchain.push(docObject.source + ':' + docObject.idConditor + '!');
      idchain.sort();
      docObject.idChain = _.join(idchain, '');

      docObject.duplicates = duplicates;
      docObject.duplicateRules = _.sortBy(allMergedRules);
      docObject.isDuplicate = (allMergedRules.length > 0);
      next();
    });
  }).catch(error => next(error));
};

module.exports = coDeduplicate;

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
