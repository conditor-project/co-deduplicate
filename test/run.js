/* global __dirname, require, process, it */

'use strict';

var
  fs = require('fs')
  , pkg = require('../package.json')
  , business = require('../index.js')
  , testData = require('./dataset/in/test.json')
  , chai = require('chai')
  , expect = chai.expect
  ;

describe(pkg.name + '/index.js', function () {

  // test sur la méthode beforeAnyJob
  describe('#beforeAnyJob', function(){

  	it('',function(done){
	  var docObject;
	  business.beforeAnyJob(function(err){


		done();
	  });
	})
  });

  // test sur la méthode doTheJob
  describe('#doTheJob', function () {

    it('', function (done) {
      var docObject;
      business.doTheJob(docObject = testData[0], function (err) {

        done();
      });
    });
  });
});
