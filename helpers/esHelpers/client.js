const config = require('@istex/config-component').get(module);
const { Client } = require('@elastic/elasticsearch');
const { logError, logInfo } = require('../logger');

module.exports = (function () {
  let instance;

  function createInstance () {
    const client = new Client(config.elastic.clients.default);
    client.on('response', (err, result) => {
      //console.dir(result.meta.request)
      //console.dir(result.body);
      if (err) {
        console.dir(err.meta.body.error);
        logError(err);
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
