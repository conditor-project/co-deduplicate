module.exports.getBaseRequest = () => {
  return {
    query: {
      bool: {
        should: [],
        minimum_should_match: 1,
      },
    },
  };
};
