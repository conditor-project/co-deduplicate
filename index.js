/* global module */
/*jslint node: true */
/*jslint indent: 2 */
"use strict";

var business = {};

business.doTheJob = function (jsonLine, cb) {
    jsonLine.canvasOK = true;
    if (jsonLine.id1 === '2b6372af-c83c-4379-944c-f1bff3ab25d8') {
      jsonLine.canvasOK = false;
      return cb({
        code: 1,
        message: 'J\'aime po cet ID là...'
      });
    } else {
      return cb();
    }
};

business.finalJob = function (docObjects, cb) {
    var err = [];
    err.push(docObjects.pop());
    docObjects[0].ending = 'finalJob';
    return cb(err);
};

module.exports = business;