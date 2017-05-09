/* global module */
/*jslint node: true */
/*jslint indent: 2 */
'use strict';

var es = require('elasticsearch'),
	_ = require('lodash'),
	fs = require('fs');

var esConf = require('./es.js');
var esMapping = require('./mapping.json');

var esClient = new es.Client({
	host: esConf.host
});

var business = {};

function dispatch(data) {

  console.log('data : ' + data.hits.hits.length);

  if (data.hits.hits.length===0){
    // si aucun hit alors on insère la donnée
	console.log('pas de doublon.');
  }
  else if (data.hits.hits.length===1){
    //si un hit alors on aggrège la donnée
	console.log('on a un doublon.');
  }
  else{
    console.log('on a plus d\'un doublon');
  }
  /**
  return new Promise(function(resolve,reject){
		return true;
  });
  **/
}


// on teste si l'entrée existe


function existNotice(jsonLine){
  return esClient.search({index : 'notices'},
    {"query" : {
      "bool":{
        "must":[
          {"match":{"title.normalized":jsonLine.titre.normalized}},
          {"match":{"doi.normalized":jsonLine.doi.normalized}}
        ]
      }
    }
  }).then(dispatch);
}



business.doTheJob = function (jsonLine, cb) {



  jsonLine.bulk = [];
  var error;

  existNotice(jsonLine).then(function(err){
  	if (err){
  	  error = {
	    errCode:1811,
		errMessage: "erreur de dédoublonnage : "+err
	  };
  	  return cb(error);
	}
	else return cb();

  });


}


// Fonction d'ajout de l'alias si nécessaire
function createAlias(aliasArgs, options, aliasCallback) {

  var error;

  // Vérification de l'existance de l'alias, création si nécessaire, ajout de l'index nouvellement créé à l'alias
  esClient.indices.existsAlias(aliasArgs, function(err, response, status) {
    if (!!!response) {
      esClient.indices.putAlias(aliasArgs, function(err, response, status) {

        if (!err) {
          options.processLogs.push("Création d'un nouvel alias OK. Status : " + status + "\n");
        } else {
         options.errLogs.push("Erreur création d'alias. Status : " + status + "\n");
          error = {
            errCode: 1703,
            errMessage: 'Erreur lors de la création de l\'alias : ' + err
          };
        }
        aliasCallback(error);
      });
    } else {
      esClient.indices.updateAliases({
        "actions": [{
        "add": aliasArgs
        }]
      }, function(err, response, status) {

        if (!err) {
          options.processLogs.push("Update d'alias OK. Status : " + status + "\n");
        } else {
          options.errLogs.push("Erreur update d'alias. Status : " + status + "\n");
          error = {
            errCode: 1703,
            errMessage: 'Erreur lors de la création de l\'alias : ' + err
          };
        }
        aliasCallback(error);
      });
    }
  });
}


// fonction préalable de création d'index si celui-ci absent.
// appelé dans beforeAnyJob

function createIndex(conditorSession,options,indexCallback){

  var reqParams = {
    index:conditorSession
  };

  var mappingExists = true;
  var error;

  esClient.indices.exists(reqParams,function(err,response,status){

    if (status !== 200) {
      options.processLogs.push("... Mapping et index introuvables, on les créé\n");
      mappingExists = false;
	} else {
      options.processLogs.push("... Mapping et index déjà existants\n");
    }

    if (!mappingExists) {

    //esMapping.settings.index = {
    //	"number_of_replicas" : 0
    //};
      reqParams.body = esMapping;

      esClient.indices.create(reqParams,function(err,response,status){
        //console.log(JSON.stringify(reqParams));
        if (status !== 200){
          options.errLogs.push("... Erreur lors de la création de l'index :\n" + err);
          error = {
            errCode: '001',
            errMessage: 'Erreur lors de la création de l\'index : ' +err
          };
          return indexCallback(error);
        }

        createAlias({
          "name": "integration",
          "index": 'notices'
        },options,function(err){
          indexCallback(err);
        });

      });

    }
    else {
      indexCallback();
	}
  });
}


business.beforeAnyJob = function(cbBefore){
  var options = {
    processLogs:[],
    errLogs:[]
  };

  var conditorSession = process.env.CONDITOR_SESSION || 'notices';
  createIndex(conditorSession,options,function(err){
  options.errLogs.push("callback createIndex, err="+err);
  return cbBefore(err,options);
  });
}


module.exports = business;