const esClient = require('../helpers/esHelpers/client').get();

module.exports = { search, deleteById, index, bulk };

function search ({ body = {}, index = '*' }) {
  return esClient
    .search({
      body,
      index,
    });
}

function deleteById (id, index, { refresh = true }) {
  return esClient
    .delete({
      id,
      index,
      refresh,
    });
}

function index (index, { body, refresh }) {
  return esClient
    .index(
      {
        index,
        body,
        refresh,
      },
    );
}

function bulk ({ body }) {
  return esClient
    .bulk(
      {
        body,
      },
    );
}
