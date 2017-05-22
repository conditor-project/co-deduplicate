'use strict';

const
  fs = require('fs'),
  pkg = require('../package.json'),
  business = require('../index.js'),
  testData = require('./dataset/in/test.json'),
  chai = require('chai'),
  expect = chai.expect,
	es = require('elasticsearch');

const esConf = require('../es.js');

const esClient = new es.Client({
	host: esConf.host,
	log: {
		type: 'file',
		level: 'trace'
	}
});


	describe(pkg.name + '/index.js', function () {

  // test sur la méthode beforeAnyJob
  	describe('#beforeAnyJob', function(){
  	
  		it('préalable : suppression index et mapping',function(done){
  			esClient.indices.exists({'index':'notices'}).then(function(data){
  					if (data===true){
  						esClient.indices.delete({'index':'notices'}).then(
  							function(){
  								done();
  							});
						}
						else
						{
							done();
						}
				});
			});
  	
  	
			it('before any job va créer l\'index et le mapping si ils n\'existent pas',function(done){
  		
  			business.beforeAnyJob(function(err){
  			
  			if (err) {
          console.log(err.errCode);
          console.log(err.errMessage);
          //process.exit(1);
        }
				console.log('post-beforeAnyJob');
  			done();
				
	  	});
	  	
		});
	
			it('insertion ou intégration de la notice 1', function (done) {
				let docObject;
				business.doTheJob(docObject = testData[0], function (err) {
          if (err) {
            console.log(err.errCode);
            console.log(err.errMessage);
            //process.exit(1);
          }
					console.log('post-doTheJob-doc1');
          done();
     
				});
			});
  });
});
