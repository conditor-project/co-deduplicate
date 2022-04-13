const esClient = require('./client').get();

module.exports = function deleteIndiceIx (indiceName) {
  return esClient
    .indices
    .exists({ index: indiceName })
    .then(({ body: doesExist }) => {
      if (!doesExist) return doesExist;
      return esClient
        .indices
        .delete({ index: indiceName })
      ;
    });
};
