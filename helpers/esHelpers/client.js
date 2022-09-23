const config = require('@istex/config-component').get(module);
const { Client } = require('@elastic/elasticsearch');
const { logError, logInfo } = require('../logger');

module.exports = (function () {
  let instance;

  function createInstance () {
    const client = new Client(config.elastic.clients.default);
    client.on('response', (err, result) => {
      if (err) {
        const failuresReasons = err?.meta?.body?.failures?.map((failure) => failure?.cause?.reason) || [];
        const failuresTypes = err?.meta?.body?.failures?.map((failure) => failure?.cause?.type) || [];
        err.failuresList = failuresReasons;
        err.failuresTypes = failuresTypes;
        logError(err);

        if (['version_conflict_engine_exception', 'script_exception'].includes(err?.meta?.body?.error?.type)) {
          logError(`[Error details] name: ${err.name}, type: ${err?.meta?.body?.error?.type}, status: ${err?.meta?.body?.status}`);
          console.dir(err?.meta?.body?.error);
        }
      }
    });

    return client;
  }

  return {
    get: function () {
      if (!instance) {
        instance = createInstance();
      }
      return instance;
    },
  };
})();
