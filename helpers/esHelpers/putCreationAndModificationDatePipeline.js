const esClient = require('./client').get();
const { readFileSync } = require('fs-extra');
const path = require('path');
const setCreationAndModificationDate = readFileSync(path.join(__dirname, './painless/setCreationAndModificationDate.painless'), 'utf8');

module.exports = function putCreationAndModificationDatePipeline () {
  return esClient
    .ingest
    .putPipeline({
      id: 'set_creation_and_modification_date',
      body: {
        description: 'This pipeline set the creation date and/or the modification.',
        processors: [
          {
            script: {
              source: setCreationAndModificationDate,
            },
          },
        ],
      },
    });
};
