/*jslint node: true */
/*jslint indent: 2 */
'use strict';

const es = require('elasticsearch'),
    _ = require('lodash'),
    debug = require('debug')('co-deduplicate');

const esConf = require('./es.js');
const esMapping = require('./mapping.json');
const scenario = require('./scenario.json');
const rules = require('./rules_certain.json');
const baseRequest = require('./base_request.json');


const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: 'trace'
    }
});

const business = {};

function insereNotice(jsonLine){

	
	let options = {index : esConf.index,type : esConf.type,refresh:true};

	debug(esConf);

	options.body= {
		'date_creation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
	};

  _.each(['titre','titrefr','titreen','auteur','auteur_init','doi','arxiv','pubmed','nnt','patentNumber',
          'ut','issn','isbn','eissn','numero','page','volume','idhal','halauthorid','orcid','researcherid',
          'viaf','datePubli'],(champs)=>{

            if (jsonLine[champs] && jsonLine[champs].value && jsonLine[champs].value!=='') {
                options.body[champs] = {'value':jsonLine[champs].value,'normalized':jsonLine[champs].value};
            }
          });
  options.body.typeConditor = jsonLine.typeConditor;
  options.body.idChain = '';
  options.body.duplicate = [];
  jsonLine.duplicate = [];

  return esClient.index(options);

}

function aggregeNotice(jsonLine, data) {


    let duplicate=[];
    let idchain=[];

    _.each(data.hits.hits,(hit)=>{
        duplicate.push({id:hit._id,rule:hit.matched_queries});
        idchain.push(hit._id);
    });

    jsonLine.duplicate = duplicate;

    let options = {index : esConf.index,type : esConf.type,refresh:true};
    
    debug(esConf);

    options.body= {
        'date_creation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
    };

    _.each(['titre','titrefr','titreen','auteur','auteur_init','doi','arxiv','pubmed','nnt','patentNumber',
            'ut','issn','isbn','eissn','numero','page','volume','idhal','halauthorid','orcid','researcherid',
            'viaf','datePubli'],(champs)=>{

                if (jsonLine[champs] && jsonLine[champs].value && jsonLine[champs].value!=='') {
                    options.body[champs] = {'value':jsonLine[champs].value,'normalized':jsonLine[champs].value};
                }
            });

    options.body.duplicate = duplicate;
    options.body.typeConditor = jsonLine.typeConditor;
    options.body.idChain = '1';
    return esClient.index(options);
}

function dispatch(jsonLine,data) {

    if (data.hits.total===0){
        //console.log('on insere');
        return insereNotice(jsonLine);
    }
    else {
        //console.log('on aggrege');
        return aggregeNotice(jsonLine,data);
    }
}

function testParameter(jsonLine,arrayParameter){

    let bool=true;
    _.each(arrayParameter,function(parameter){
        if (_.get(jsonLine,parameter)===undefined || _.get(jsonLine,parameter).trim()==='') bool = false ;
    });
    return bool;
}

function interprete(jsonLine,query,type){
    const newQuery ={
        bool: {
            must:null,
            _name:query.bool._name
    }};
    
    newQuery.bool.must =  _.map(query.bool.must,(value)=>{
        let match = {'match':null};
        match.match = _.mapValues(value.match,(pattern)=>{
            return _.get(jsonLine,pattern);
        });
        return match;
    });
   
    newQuery.bool.must.push({'nested':{'path':'typeConditor','query':{'bool':{'must':[{'match':{'typeConditor.type':type}}]}}}});
    
    return newQuery;
  
}

// on crée la requete puis on teste si l'entrée existe
function existNotice(jsonLine){
    
    let request = _.cloneDeep(baseRequest);

    _.each(jsonLine.typeConditor, (type)=>{
        if (type && type.type && scenario[type.type]){
            _.each(scenario[type.type],(rule)=>{
                if (rules[rule] && testParameter(jsonLine,rules[rule].non_empty)) {
                        request.query.bool.should.push(interprete(jsonLine,rules[rule].query,type.type));
                    }
            });
        }
        else {
            _.each(scenario.Article,(rule)=>{
                if (rules[rule] && testParameter(jsonLine,rules[rule].non_empty)) {
                        request.query.bool.should.push(interprete(jsonLine,rules[rule].query,'Article'));
                    }
            });
        }
    });

    //console.log(JSON.stringify(request));

    return esClient.search({
        index: esConf.index,
        body : request
    }).then(dispatch.bind(null,jsonLine),function(error){
        console.error(error);
    });

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
        if (status !== '200') {
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