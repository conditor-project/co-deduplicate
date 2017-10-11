/*jslint node: true */
/*jslint indent: 2 */
'use strict';

const es = require('elasticsearch'),
    _ = require('lodash'),
    fs = require('fs'),
    debug = require('debug')('co-deduplicate');

const esConf = require('./es_poc.js');
const esMapping = require('./mapping_poc.json');

const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: 'debug'
    }
});

const business = {};
const nbRegles = 6;

function insereNotice(jsonLine){

	
	let options = {index : esConf.index,type : esConf.type,refresh:true};

	debug(esConf);

	options.body= {
		'date_creation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
    'source': []
	};
  let source = {
    'name':jsonLine.source,
    'path':jsonLine.path,
    'date_integration':new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
    'champs': {}
  };

  _.each(['titre','titrefr','titreen','auteur','auteur_init','doi','arxiv','pubmed','nnt','patentNumber',
          'ut','issn','isbn','eissn','numero','page','volume','idhal','halauthorid','orcid','researcherid',
          'viaf','datePubli'],(champs)=>{

            if (jsonLine[champs] && jsonLine[champs].value && jsonLine[champs].value!=='') {
                options.body[champs] = {'value':jsonLine[champs].value,'normalized':jsonLine[champs].value};
                source.champs[champs] = {'value':jsonLine[champs].value,'normalized':jsonLine[champs].value};
            }
          });

  options.body.conditor_ident = jsonLine.conditor_ident;
  source.champs.conditor_ident = jsonLine.conditor_ident;
  options.body.source.push(source);

  return esClient.index(options);

}

function aggregeNotice(jsonLine, data) {

    let source = data.hits.hits[0]._source;
    let id_es = data.hits.hits[0]._id;

    let options = { index: esConf.index, type: esConf.type, id: id_es, refresh: true };

    let sourceData = source.source;

    let future_source = [];

    _.each(sourceData, function(arraysource) {
        if (arraysource.name !== jsonLine.source) future_source.push(arraysource);
    });

    future_source.push({
        'name': jsonLine.source,
        'path': jsonLine.path,
        'date_integration': new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''),
        'champs': {
            'titre': {
                'value': jsonLine.titre.value,
                'normalized':jsonLine.titre.value
            },
            'titrefr': {
                'value': jsonLine.titrefr.value,
                'normalized':jsonLine.titrefr.value
            },
            'titreen': {
                'value': jsonLine.titreen.value,
                'normalized': jsonLine.titreen.value
            },
            'auteur': {
                'value': jsonLine.auteur.value,
                'normalized':jsonLine.auteur.value
            },
            'auteur_init': {
                'value': jsonLine.auteur_init.value,
                'normalized':jsonLine.auteur_init.value
            },
            'doi': {
                'value': jsonLine.doi.value,
                'normalized':jsonLine.doi.value
            },
            'arxiv': {
                'value': jsonLine.arxiv.value,
                'normalized':jsonLine.arxiv.value
            },
            'pubmed': {
                'value': jsonLine.pubmed.value,
                'normalized' : jsonLine.pubmed.value
            },
            'nnt': {
                'value': jsonLine.nnt.value,
                'normalized': jsonLine.nnt.value
            },
            'patentNumber': {
                'value': jsonLine.patentNumber.value,
                'normalized': jsonLine.patentNumber.value 
            },
            'ut': {
                'value': jsonLine.ut.value,
                'normalized': jsonLine.ut.value
            },
            'issn': {
                'value': jsonLine.issn.value,
                'normalized': jsonLine.issn.value
            },
            'eissn': {
                'value': jsonLine.eissn.value,
                'normalized': jsonLine.eissn.value
            },
            'isbn': {
                'value': jsonLine.isbn.value,
                'normalized': jsonLine.isbn.value
            },
            'numero': {
                'value': jsonLine.numero.value,
                'normalized':jsonLine.numero.value
            },
            'volume': {
                'value': jsonLine.volume.value,
                'normalized':jsonLine.volume.value
            },
            'page': {
                'value': jsonLine.page.value,
                'normalized':jsonLine.page.value
            },
            'idhal': {
                'value': jsonLine.idhal.value,
                'normalized':jsonLine.idhal.value
            },
            'halauthorid': {
                'value': jsonLine.halauthorid.value,
                'normalized':jsonLine.halauthorid.value
            },
            'orcid': {
                'value': jsonLine.orcid.value,
                'normalized':jsonLine.orcid.value
            },
            'researcherid': {
                'value': jsonLine.researcherid.value,
                'normalized':jsonLine.researcherid.value
            },
            'viaf': {
                'value': jsonLine.viaf.value,
                'normalized':jsonLine.viaf.value
            },
            'typeDocument': {
                'value': jsonLine.typeDocument.value,
            },
            'titreSource':{
                'value':jsonLine.titreSource.value,
                'normalized':jsonLine.titreSource.value
            },
            'datePubli':{
                'value':jsonLine.datePubli.value
            },
            'typeConditor':{
                'value':jsonLine.typeConditor.value
            },
            'conditor_ident':{
                'value':jsonLine.conditor_ident
            }

            
        }
    });

    source.source = future_source;

    options.body = source;

    return esClient.index(options);
}

function dispatch(jsonLine,data) {

  if (!data && jsonLine.conditor_ident<nbRegles) {
    debug('on continue à chercher (le test précédent était inutile ou en erreur)');
    return existNotice(jsonLine);
  }
  else if (jsonLine.conditor_ident===nbRegles && (!data || data.hits.hits.length===0)){
    // si aucun hit alors on insère la donnée
		jsonLine.conditor_ident=99;
		debug('pas de doublon.');
		return insereNotice(jsonLine);
  }
  else if (data.hits.hits.length===0 && jsonLine.conditor_ident<=nbRegles){
  	debug('on continue à chercher');
  	return existNotice(jsonLine);
	}
  else if (data.hits.hits.length===1){
    //si un hit alors on aggrège la donnée
	debug('on a un doublon.');
  debug('nb occurence : ' + data.hits.hits.length);
  return aggregeNotice(jsonLine,data);
  }
  else{
    debug('on a plus d\'un doublon');
    debug('nb occurence : ' + data.hits.hits.length);
  }
}

// on teste si l'entrée existe
function existNotice(jsonLine){
	
	if (jsonLine.conditor_ident===0) {
		
		jsonLine.conditor_ident=1;
		debug('test sur titre+doi');

    const fieldsOK = jsonLine.titre && jsonLine.titre.value && jsonLine.titre.value !==''
      && jsonLine.doi && jsonLine.doi.value && jsonLine.doi.value !=='';
    if (!fieldsOK) return dispatch(jsonLine);

		return esClient.search({
			index: esConf.index,
			body: {
				'query': {
					'bool': {
						'should': [
							{
								'bool': {
									'must': [
										{'match': {'titre.normalized': jsonLine.titre.value}},
										{'match': {'doi.normalized': jsonLine.doi.value}}
									]
								}
							},
							{
								'bool': {
									'must': [
										{'match': {'source.champs.titre.normalized': jsonLine.titre.value}},
										{'match': {'source.champs.doi.normalized': jsonLine.doi.value}}
									]
								}
							}
						],
						'minimum_should_match': 1
					}
				}
			}
			
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.error(error);
		});
	}
	else if (jsonLine.conditor_ident===1){
		
		jsonLine.conditor_ident=2;
		debug('test sur titre+volume+numero+issn');
    
    const fieldsOK = jsonLine.titre && jsonLine.titre.value && jsonLine.titre.value !==''
      && jsonLine.volume && jsonLine.volume.value && jsonLine.volume.value !==''
      && jsonLine.numero && jsonLine.numero.value && jsonLine.numero.value !==''
      && jsonLine.issn && jsonLine.issn.value && jsonLine.issn.value !=='';
    if (!fieldsOK) return dispatch(jsonLine);

		return esClient.search({
			index: esConf.index,
			body: {
				'query' :{
					'bool':{
						'should':[
							{
								'bool': {
									'must': [
										{'match': {'titre.normalized': jsonLine.titre.value}},
										{'match': {'volume.normalized': jsonLine.volume.value}},
										{'match': {'numero.normalized': jsonLine.numero.value}},
										{'match': {'issn.normalized': jsonLine.issn.value}}
									]
								}
							},
							{
								'bool': {
									'must': [
										{'match': {'source.champs.titre.normalized': jsonLine.titre.value}},
										{'match': {'source.champs.volume.normalized': jsonLine.volume.value}},
										{'match': {'source.champs.numero.normalized': jsonLine.numero.value}},
										{'match': {'source.champs.issn.normalized': jsonLine.issn.value}}
									]
								}
							}
						],
						'minimum_should_match':1
					}
				}
			}
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.error(error);
		});
	}
	else if (jsonLine.conditor_ident===2){
		
		jsonLine.conditor_ident=3;
		debug('test sur doi');

    const fieldsOK = jsonLine.doi && jsonLine.doi.value && jsonLine.doi.value !=='';
    if (!fieldsOK) return dispatch(jsonLine);

		return esClient.search({
			index: esConf.index,
			body: {
				'query': {
					'bool': {
						'should': [
							{
								'bool': {
									'must': [
										{'match': {'doi.normalized': jsonLine.doi.value}}
									]
								}
							},
							{
								'bool': {
									'must': [
										{'match': {'source.champs.doi.value': jsonLine.doi.value}}
									]
								}
							}
						],
						'minimum_should_match': 1
					}
				}
			}
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.error(error);
		});
	}
	else if (jsonLine.conditor_ident===3){
		
		jsonLine.conditor_ident=4;
		debug('test sur titre+auteur+issn');

    const fieldsOK = jsonLine.titre && jsonLine.titre.value && jsonLine.titre.value !==''
      && jsonLine.auteur && jsonLine.auteur.value && jsonLine.auteur.value !==''
      && jsonLine.issn && jsonLine.issn.value && jsonLine.issn.value !=='';
    if (!fieldsOK) return dispatch(jsonLine);

		return esClient.search({
			index: esConf.index,
			body: {
				'query': {
					'bool': {
						'should': [
							{
								'bool': {
									'must': [
										{'match': {'titre.normalized': jsonLine.titre.value}},
										{'match': {'auteur.normalized': jsonLine.auteur.value}},
										{'match': {'issn.normalized': jsonLine.issn.value}}
									]
								}
							},
							{
								'bool': {
									'must': [
										{'match': {'source.champs.titre.normalized': jsonLine.titre.value}},
										{'match': {'source.champs.auteur.normalized': jsonLine.auteur.value}},
										{'match': {'source.champs.issn.normalized': jsonLine.issn.value}}
									]
								}
							}
						],
						'minimum_should_match': 1
					}
				}
			}
	
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.error(error);
		});
	}
	else if (jsonLine.conditor_ident===4){
		
		jsonLine.conditor_ident=5;
		debug('test sur titre+auteur_init+issn');

    const fieldsOK = jsonLine.titre && jsonLine.titre.value && jsonLine.titre.value !==''
      && jsonLine.auteur_init && jsonLine.auteur_init.value && jsonLine.auteur_init.value !==''
      && jsonLine.issn && jsonLine.issn.value && jsonLine.issn.value !=='';
    if (!fieldsOK) return dispatch(jsonLine);

		return esClient.search({
			index: esConf.index,
			body: {
				'query': {
					'bool': {
						'should': [
							{
								'bool': {
									'must': [
										{'match': {'titre.normalized': jsonLine.titre.value}},
										{'match': {'auteur_init.normalized': jsonLine.auteur_init.value}},
										{'match': {'issn.normalized': jsonLine.issn.value}}
									]
								}
							},
							{
								'bool': {
									'must': [
										{'match': {'source.champs.titre.normalized': jsonLine.titre.value}},
										{'match': {'source.champs.auteur_init.normalized': jsonLine.auteur_init.value}},
										{'match': {'source.champs.issn.normalized': jsonLine.issn.value}}
									]
								}
							}
						],
						'minimum_should_match': 1
					}
				}
			}
		
		}).then(dispatch.bind(null, jsonLine), function (error) {
			console.error(error);
		});
	}
	else if (jsonLine.conditor_ident===5){
		
		jsonLine.conditor_ident=6;
		debug('test sur issn+volume+numero+page');
		
    const fieldsOK = jsonLine.page && jsonLine.page.value && jsonLine.page.value !==''
      && jsonLine.volume && jsonLine.volume.value && jsonLine.volume.value !==''
      && jsonLine.numero && jsonLine.numero.value && jsonLine.numero.value !==''
      && jsonLine.issn && jsonLine.issn.value && jsonLine.issn.value !=='';
    if (!fieldsOK) return dispatch(jsonLine);

    return esClient.search({
			index: esConf.index,
			body: {
				'query': {
					'bool': {
						'should': [
							{
								'bool': {
									'must': [
										{'match': {'issn.normalized': jsonLine.issn.value}},
										{'match': {'volume.normalized': jsonLine.volume.value}},
										{'match': {'numero.normalized': jsonLine.numero.value}},
										{'match': {'page.normalized': jsonLine.page.value}}
									]
								}
							},
							{
								'bool': {
									'must': [
										{'match': {'source.champs.issn.normalized': jsonLine.issn.value}},
										{'match': {'source.champs.volume.normalized': jsonLine.volume.value}},
										{'match': {'source.champs.numero.normalized': jsonLine.numero.value}},
										{'match': {'source.champs.page.normalized': jsonLine.page.value}}
									]
								}
							}
						],
						'minimum_should_match': 1
					}
				}
			}
		
		}).then(dispatch.bind(null, jsonLine), function (error) {
			debug('Error :'+error);
		});
	}
}



business.doTheJob = function(jsonLine, cb) {

    let error;
    jsonLine.conditor_ident = 0;

    return existNotice(jsonLine).then(function(result) {

            //debug(result);
            jsonLine.id_elasticsearch = result._id;
            debug(jsonLine);
            return cb();



        },
        function(err) {
            if (err) {
                error = {
                    errCode: 1811,
                    errMessage: 'erreur de dédoublonnage : ' + err
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
        if (status !== "200") {
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
                        errCode: 1704,
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

function createIndex(conditorSession, options, indexCallback) {

    let reqParams = {
        index: conditorSession
    };

    let mappingExists = true;
    let error;

    esClient.indices.exists(reqParams, function(err, response, status) {

        if (status !== 200) {
            options.processLogs.push('... Mapping et index introuvables, on les créé\n');
            mappingExists = false;
        } else {
            options.processLogs.push('... Mapping et index déjà existants\n');
        }

        if (!mappingExists) {


            esMapping.settings.index = {
                    'number_of_replicas': 0
            };

            reqParams.body = esMapping;

            esClient.indices.create(reqParams, function(err, response, status) {
                //debug(JSON.stringify(reqParams));
                if (status !== 200) {
                    options.errLogs.push('... Erreur lors de la création de l\'index :\n' + err);
                    error = {
                        errCode: '001',
                        errMessage: 'Erreur lors de la création de l\'index : ' + err
                    };
                    return indexCallback(error);
                }

                createAlias({
                    index: esConf.index,
                    name: 'integration',
                    body: { 'actions': { 'add': { 'index': esConf.index, 'alias': 'integration' } } }
                }, options, function(err) {
                    indexCallback(err);
                });

            });

        } else {
            indexCallback();
        }
    });
}


business.beforeAnyJob = function(cbBefore) {
    let options = {
        processLogs: [],
        errLogs: []
    };

    let conditorSession = process.env.CONDITOR_SESSION || esConf.index;
    createIndex(conditorSession, options, function(err) {
        options.errLogs.push('callback createIndex, err=' + err);
        return cbBefore(err, options);
    });
}


module.exports = business;