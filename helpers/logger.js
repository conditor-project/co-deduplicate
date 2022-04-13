'use strict';

const
  _ = require('lodash');
const trans = require('./trans');
const colors = require('./myColors');
const packageJson = require('../package.json')
;

const appName = _.get(packageJson, 'name', 'myApp');

module.exports.logInfo = logInfo;
module.exports.logError = logError;
module.exports.logWarning = logWarning;
module.exports.logDebug = logDebug;
module.exports.logSuccess = logSuccess;
module.exports.getUtcDate = getUtcDate;

function logError (err) {
  const message = typeof err === 'string' ? arguments : [err.message || '', err];
  console.error('%s [%s] [%s] %s',
    appName.bold.danger,
    'Error',
    getUtcDate(),
    ...(_.map(message, trans)),
  )
  ;
}

function logSuccess () {
  console.info('%s [%s] [%s] %s',
    appName.bold.success,
    'Success',
    getUtcDate(),
    ...(_.map(arguments, trans)),
  );
}

function logInfo () {
  console.info('%s [%s] [%s] %s',
    appName.bold.info,
    'Info',
    getUtcDate(),
    ...(_.map(arguments, trans)),
  );
}

function logWarning (err) {
  if ((process.env.NODE_ENV === 'test' && logWarning.doWarn !== true) || logWarning.doWarn === false) return;
  const message = typeof err === 'string' ? arguments : [err.message || '', err];
  console.warn('%s [%s] [%s] %s',
    appName.bold.warning,
    'Warning',
    getUtcDate(),
    ...(_.map(message, trans)),
  );
}

function logDebug () {
  if (['test', 'production'].includes(process.env.NODE_ENV)) return;
  console.info('%s [%s] [%s] %s',
    appName.bold.primary,
    'Debug',
    getUtcDate(),
    ...(_.map(arguments, trans)),
  );
}

function getUtcDate (date = Date.now()) {
  return new Date(date).toLocaleString(undefined, { timeZoneName: 'short' });
}
