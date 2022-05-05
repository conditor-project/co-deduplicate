const esClient = require('./client').get();

module.exports = function createIndiceNx (indiceName, indiceConfig = {}) {
  return esClient
    .indices
    .exists({ index: indiceName })
    .then(({ body: doesExist }) => {
      if (doesExist) return doesExist;
      return esClient
        .indices
        .create(
          {
            index: indiceName,
            body: indiceConfig
          },
        );
    });
};
