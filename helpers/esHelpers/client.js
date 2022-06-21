const config = require('@istex/config-component').get(module);
const { Client } = require('@elastic/elasticsearch');
const { logError, logInfo } = require('../logger');

module.exports = (function () {
  let instance;

  function createInstance () {
    const client = new Client(config.elastic.clients.default);
    client.on('response', (err, result) => {
      if (err) {
        const failuresList = err?.meta?.body?.failures?.map((failure) => failure?.cause?.reason) || [];
        err.failuresList = failuresList;
        logError(err);
        if (err?.meta?.body?.error?.type === 'script_exception') {
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
