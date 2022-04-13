'use strict';

module.exports = trans;

const messages = require('../resources/messages.json');

function trans (input) {
  if (typeof input === 'function') return;
  return (input && messages[input]) || input;
}
