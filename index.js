/*jslint node: true */
/*jslint indent: 2 */
'use strict';

const es = require('elasticsearch'),
	_ = require('lodash'),
	fs = require('fs');

const esConf = require('./es.js');
const esMapping = require('./mapping.json');

const esClient = new es.Client({
	host: esConf.host,
	log: {
		type: 'file',
		level: 'trace'
	}
});

const business = {};


function insereNotice(jsonLine){

	
	let options = {index : 'notices',type : 'notice',refresh:true};

	console.log(esConf);
	
	
	options.body= {
		'titre':{'value':jsonLine.titre.value ,
							'normalized':jsonLine.titre.normalized},
		'auteur':{'value':jsonLine.auteur.value,
							'normalized':jsonLine.auteur.normalized},
		'auteur_init':{'value':jsonLine.auteur_init.value,
							'normalized':jsonLine.auteur_init.normalized},
		'doi':{'value':jsonLine.doi.value,
						'normalized':jsonLine.doi.normalized},
		'issn':{'value':jsonLine.issn.value,
						'normalized':jsonLine.issn.normalized},
		'numero':{'value':jsonLine.numero.value,
							'normalized':jsonLine.numero.normalized},
		'volume':{'value':jsonLine.volume.value,
							'normalized':jsonLine.volume.normalized},
		'page':{'value':jsonLine.page.value,
						'normalized':jsonLine.page.normalized},
		'source':[{'name':jsonLine.source,
			'path':jsonLine.path,
			'date_integration':new Date().toISOString().replace(/T/,' ').replace(/\..+/,'')
			}],
		'date_creation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,'')
	};
	
	
	//console.log(doc);
	
	return esClient.index(options);

}

function aggregeNotice(jsonLine,data){

	let source = data.hits.hits[0]._source;
	let id_es=data.hits.hits[0]._id;
	
	let options = {index:'notices',type:'notice',id:id_es,refresh:true};
	
	let sourceData = source.source;
	
	let future_source=[];
	
	_.each(sourceData,function(arraysource){
		if (arraysource.name!==jsonLine.source) future_source.push(arraysource);
	});
	
	future_source.push({'name':jsonLine.source,'path':jsonLine.path,'date_integration':new Date().toISOString().replace(/T/,' ').replace(/\..+/,'')});
	
	source.source=future_source;
	
	options.body=source;
	
	return esClient.index(options);
}


function dispatch(jsonLine,data) {

  console.log('nb occurence : ' + data.hits.hits.length);

  if (data.hits.hits.length===0 && jsonLine.conditor_ident===6){
    // si aucun hit alors on insère la donnée
	console.log('pas de doublon.');
	return insereNotice(jsonLine);
  }
  else if (data.hits.hits.length===0 && jsonLine.conditor_ident<=6){
  	console.log('on continue à chercher');
  	return existNotice(jsonLine);
	}
  else if (data.hits.hits.length===1){
    //si un hit alors on aggrège la donnée
	console.log('on a un doublon.');
	return aggregeNotice(jsonLine,data);
  }
  else{
    console.log('on a plus d\'un doublon');
  }
 
}


// on teste si l'entrée existe


function existNotice(jsonLine){
	
	if (jsonLine.conditor_ident==0) {
		
		jsonLine.conditor_ident=1;
		
		return esClient.search({
			index: 'notices',
			query: {
				'bool': {
					'must': [
						{'match': {'title.normalized': jsonLine.titre.normalized}},
						{'match': {'doi.normalized': jsonLine.doi.normalized}}
					]
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.log(error);
		});
	}
	else if (jsonLine.conditor_ident==1){
		
		jsonLine.conditor_ident=2;
		
		return esClient.search({
			index: 'notices',
			query: {
				'bool': {
					'must': [
						{'match': {'title.normalized': jsonLine.titre.normalized}},
						{'match': {'volume.normalized': jsonLine.volume.normalized}},
						{'match': {'numero.normalized': jsonLine.numero.normalized}},
						{'match': {'issn.normalized': jsonLine.issn.normalized}}
					]
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.log(error);
		});
	}
	else if (jsonLine.conditor_ident==2){
		
		jsonLine.conditor_ident=3;
		
		return esClient.search({
			index: 'notices',
			query: {
				'bool': {
					'must': [
						{'match': {'doi.normalized': jsonLine.doi.normalized}}
					]
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.log(error);
		});
	}
	else if (jsonLine.conditor_ident==3){
		
		jsonLine.conditor_ident=4;
		
		return esClient.search({
			index: 'notices',
			query: {
				'bool': {
					'must': [
						{'match': {'title.normalized': jsonLine.titre.normalized}},
						{'match': {'auteur.normalized': jsonLine.auteur.normalized}},
						{'match': {'issn.normalized': jsonLine.issn.normalized}}
					]
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.log(error);
		});
	}
	else if (jsonLine.conditor_ident==4){
		
		jsonLine.conditor_ident=5;
		
		return esClient.search({
			index: 'notices',
			query: {
				'bool': {
					'must': [
						{'match': {'title.normalized': jsonLine.titre.normalized}},
						{'match': {'auteur_init.normalized': jsonLine.auteur_init.normalized}},
						{'match': {'issn.normalized': jsonLine.issn.normalized}}
					]
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.log(error);
		});
	}
	else if (jsonLine.conditor_ident==5){
		
		jsonLine.conditor_ident=6;
		
		return esClient.search({
			index: 'notices',
			query: {
				'bool': {
					'must': [
						{'match': {'issn.normalized': jsonLine.issn.normalized}},
						{'match': {'volume.normalized': jsonLine.volume.normalized}},
						{'match': {'numero.normalized': jsonLine.numero.normalized}},
						{'match': {'page.normalized': jsonLine.page.normalized}}
					]
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.log(error);
		});
	}
}



business.doTheJob = function (jsonLine, cb) {
	
  let error;
  jsonLine.conditor_ident=0;

  existNotice(jsonLine).then(function(result) {
	
  		console.log(result);
  		return cb();
  	
		},
	function(err){
			if (err){
				error = {
					errCode:1811,
					errMessage: 'erreur de dédoublonnage : '+err
				};
			return cb(error);
		}
	});


}


// Fonction d'ajout de l'alias si nécessaire
function createAlias(aliasArgs, options, aliasCallback) {

  let error;

  // Vérification de l'existance de l'alias, création si nécessaire, ajout de l'index nouvellement créé à l'alias
  esClient.indices.existsAlias(aliasArgs, function(err, response, status) {
    if (!response) {
      esClient.indices.putAlias(aliasArgs, function(err, response, status) {

        if (!err) {
          options.processLogs.push('Création d\'un nouvel alias OK. Status : ' + status + '\n');
        } else {
         options.errLogs.push('Erreur création d\'alias. Status : ' + status + '\n');
          error = {
            errCode: 1703,
            errMessage: 'Erreur lors de la création de l\'alias : ' + err
          };
        }
        aliasCallback(error);
      });
    } else {
      esClient.indices.updateAliases({
        'actions': [{
        'add': aliasArgs
        }]
      }, function(err, response, status) {

        if (!err) {
          options.processLogs.push('Update d\'alias OK. Status : ' + status + '\n');
        } else {
          options.errLogs.push('Erreur update d\'alias. Status : ' + status + '\n');
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

  let reqParams = {
    index:conditorSession
  };

  let mappingExists = true;
  let error;

  esClient.indices.exists(reqParams,function(err,response,status){

    if (status !== 200) {
      options.processLogs.push('... Mapping et index introuvables, on les créé\n');
      mappingExists = false;
	} else {
      options.processLogs.push('... Mapping et index déjà existants\n');
    }

    if (!mappingExists) {

    //esMapping.settings.index = {
    //	'number_of_replicas' : 0
    //};
      reqParams.body = esMapping;

      esClient.indices.create(reqParams,function(err,response,status){
        //console.log(JSON.stringify(reqParams));
        if (status !== 200){
          options.errLogs.push('... Erreur lors de la création de l\'index :\n' + err);
          error = {
            errCode: '001',
            errMessage: 'Erreur lors de la création de l\'index : ' +err
          };
          return indexCallback(error);
        }

        createAlias({
          'name': 'integration',
          'index': 'notices'
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
  let options = {
    processLogs:[],
    errLogs:[]
  };

  let conditorSession = process.env.CONDITOR_SESSION || 'notices';
  createIndex(conditorSession,options,function(err){
  options.errLogs.push('callback createIndex, err='+err);
  return cbBefore(err,options);
  });
}


module.exports = business;