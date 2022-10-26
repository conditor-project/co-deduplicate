module.exports.getBaseRequest = () => {
  return {
    bool: {
      should: [],
      minimum_should_match: 1,
    },
  };
};
