/*jslint node: true */
/*jslint indent: 2 */
'use strict';

const es = require('elasticsearch'),
    _ = require('lodash'),
    debug = require('debug')('co-deduplicate');

const Promise = require('bluebird');
const nanoid = require('nanoid');
const esConf = require('./es.js');
const esMapping = require('./mapping.json');
const scenario = require('./scenario.json');
const rules = require('./rules_certain.json');
const baseRequest = require('./base_request.json');
const provider_rules = require('./rules_provider.json');
//en attendant un co-conf 
const listeChamps =['titre','titrefr','titreen','auteur','auteur_init','doi','arxiv','pubmed','nnt','patentNumber',
'ut','issn','isbn','eissn','numero','page','volume','idhal','idprodinra','orcid','researcherid',
'viaf','datePubli'];


const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: 'debug'
    }
});

const business = {};

function insereNotice(jsonLine){

	
	let options = {index : esConf.index,type : esConf.type,refresh:true};

	debug(esConf);

	options.body= {
		'date_creation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
	};

  _.each(listeChamps,(champs)=>{

            if (jsonLine[champs] && jsonLine[champs].value && jsonLine[champs].value!=='') {
                options.body[champs] ={'value':jsonLine[champs].value,'normalized':jsonLine[champs].value};
            }
          });
  options.body.path = jsonLine.path;
  options.body.halautorid = jsonLine.halautorid;
  options.body.source = jsonLine.source;
  options.body.typeConditor = [];
  options.body.idConditor = jsonLine.idConditor;
  _.each(jsonLine.typeConditor,(typeCond)=>{
    options.body.typeConditor.push({'type':typeCond.type,'raw':typeCond.type});
  });
  options.body.idChain = jsonLine.source+':'+jsonLine.idConditor;
  options.body.duplicate = [];
  jsonLine.duplicate = [];
  //console.log(JSON.stringify(options));
  return esClient.index(options);

}

function aggregeNotice(jsonLine, data) {


    let duplicate=[];
    let idchain=[];

    idchain.push(jsonLine.source+':'+jsonLine.idConditor);
    _.each(data.hits.hits,(hit)=>{
        duplicate.push({id:hit._source.idConditor,rule:hit.matched_queries});
        idchain=_.union(idchain,hit._source.idChain.split('!'));
    });

    idchain.sort();
    jsonLine.duplicate = duplicate;

    let options = {index : esConf.index,type : esConf.type,refresh:true};
    
    debug(esConf);

    options.body= {
        'date_creation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
    };

    _.each(listeChamps,(champs)=>{

                if (jsonLine[champs] && jsonLine[champs].value && jsonLine[champs].value!=='') {
                    options.body[champs] ={'value':jsonLine[champs].value,'normalized':jsonLine[champs].value};
                }
            });
    options.body.path = jsonLine.path;
    options.body.halautorid = jsonLine.halautorid;
    options.body.source = jsonLine.source;
    options.body.duplicate = duplicate;
    options.body.typeConditor = [];
    options.body.idConditor = jsonLine.idConditor;
    _.each(jsonLine.typeConditor,(typeCond)=>{
        options.body.typeConditor.push({'type':typeCond.type,'raw':typeCond.type});
    });
    options.body.idChain = _.join(idchain,'!');
    jsonLine.idChain = options.body.idChain;
    //console.log(JSON.stringify(options));
    return esClient.index(options);
}

function propagate(jsonLine,data,result){


    let options;
    let update;
    let body=[];
    let option;
    let arrayDuplicate;

    _.each(data.hits.hits,(hit)=>{
       
        options={update:{_index:esConf.index,_type:esConf.type,_id:hit._id}};
        //constitution du duplicate

        _.each(jsonLine.duplicate,(duplicate)=>{
            if (duplicate.id===hit._source.idConditor){
                arrayDuplicate=hit._source.duplicate;
                arrayDuplicate.push({id:result._id,rule:duplicate.rule});
            }
        });

        update={doc:{idChain:jsonLine.idChain,duplicate:arrayDuplicate}};
        body.push(options);
        body.push(update);
        


    });
    option={body:body};
    return esClient.bulk(option);
}

function dispatch(jsonLine,data) {

    // creation de l'id
    jsonLine.idConditor = nanoid();
    
    if (data.hits.total===0){
        //console.log('on insere');
        return insereNotice(jsonLine);
    }
    else {
        //console.log('on aggrege');
        return aggregeNotice(jsonLine,data)
                .then(propagate.bind(null,jsonLine,data),(error)=>{
                    console.error(error);
        });
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
    
    let rulename;
    if (type.trim()!=='')
        rulename = type+' : '+query.bool._name;
    else 
        rulename = query.bool._name;

    const newQuery ={
        bool: {
            must:null,
            _name:rulename
    }};
    
    newQuery.bool.must =  _.map(query.bool.must,(value)=>{
        let match = {'match':null};
        match.match = _.mapValues(value.match,(pattern)=>{
            return _.get(jsonLine,pattern);
        });
        return match;
    });
   
    if (type!=='')
        newQuery.bool.must.push({'nested':{'path':'typeConditor','query':{'bool':{'must':[{'match':{'typeConditor.type':type}}]}}}});
    
    return newQuery;
  
}

// on crée la requete puis on teste si l'entrée existe
function existNotice(jsonLine){
    
    return Promise.try(function(){
        let request = _.cloneDeep(baseRequest);

        // construction des règles par scénarii
        _.each(jsonLine.typeConditor, (type)=>{

            if (type && type.type && scenario[type.type]){
                _.each(scenario[type.type],(rule)=>{
                    if (rules[rule] && testParameter(jsonLine,rules[rule].non_empty)) {
                            request.query.bool.should.push(interprete(jsonLine,rules[rule].query,type.type));
                        }
                });
            }
        });

        if (request.query.bool.should.length===0){
            throw new Error('Métadatas insuffisantes pour traiter la notice.');
        }
        else{
        
            // construction des règles uniquement sur l'identifiant de la source
            _.each(provider_rules,(provider_rule)=>{
                if (jsonLine.source.trim()===provider_rule.source.trim() && testParameter(jsonLine,provider_rule.non_empty)){
                    request.query.bool.should.push(interprete(jsonLine,provider_rule.query,''))
                }
            });
            //console.log(JSON.stringify(request));

            return esClient.search({
                index: esConf.index,
                body : request
            }).then(dispatch.bind(null,jsonLine));
        }
    });

}



business.doTheJob = function(jsonLine, cb) {

    let error;
    jsonLine.conditor_ident = 0;

    existNotice(jsonLine).catch(function(e){
        error = {
            errCode: 3,
            errMessage: 'erreur de dédoublonnage : ' + e
        };
        jsonLine.error = error;
        cb(error);
    }).then(function(result) {

            //debug(result);
            //debug(jsonLine);
            return cb();

    });
};


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