module.exports = {
  extends: ['semistandard', 'standard-jsx'],
  rules: {
    complexity: ['error', 10],
    'comma-dangle': ['error', 'always-multiline'],
  },
  env: {
    mocha: true,
  },
};
