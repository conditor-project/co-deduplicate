const { _ } = require('lodash');
const { deduplicate: { scenario, rules } } = require('corhal-config');
const { getBaseRequest } = require('./getBaseRequest');

module.exports = { buildQuery, buildQueryFromRule };

function buildQuery (docObject) {
  const request = getBaseRequest();

  if (scenario[docObject.business.duplicateGenre]) {
    _.each(rules, (rule) => {
      if (_.includes(scenario[docObject.business.duplicateGenre], rule.rule) &&
          validateRequiredAndForbiddenParameters(docObject, rule)
      ) {
        request.query.bool.should.push(buildQueryFromRule(docObject, rule));
      }
    });
  }

  request.query.bool.must_not = { term: { _id: docObject.technical.internalId } };

  return request;
}

function validateRequiredAndForbiddenParameters (docObject, rule) {
  let isAllParametersValid = true;
  _.each(rule.non_empty, function (requiredParameter) {
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

function buildQueryFromRule (docObject, rule) {
  let ruleName;
  const type = docObject.business.duplicateGenre;
  if (_isContentString(type)) {
    ruleName = type + ' : ' + rule.query.bool._name;
  } else {
    ruleName = rule.query.bool._name;
  }
  const newQuery = {
    bool: {
      _name: ruleName,
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

  if (_isContentString(type)) {
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

// Helpers

function _isContentString (value) {
  return typeof value === 'string' && value.trim() !== '';
}
