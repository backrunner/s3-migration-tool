module.exports = {
  extends: ['alloy', 'alloy/typescript', 'prettier'],
  env: {
    node: true,
  },
  rules: {
    complexity: 0,
    'max-params': 0,
    'no-console': 0,
  },
};
