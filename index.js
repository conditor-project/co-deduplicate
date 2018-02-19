'use strict';

const es = require('elasticsearch'),
    _ = require('lodash'),
    debug = require('debug')('co-deduplicate');

const Promise = require('bluebird');
const generate = require('nanoid/generate');
const esConf = require('co-config/es.js');
const esMapping = require('co-config/mapping.json');
const scenario = require('co-config/scenario.json');
//const scenario = require('./scenario_newname_suppression.json');
const rules = require('co-config/rules_certain.json');
//const rules = require('./rules_perline_newname_suppression_indent.json');
const baseRequest = require('co-config/base_request.json');
const provider_rules = require('co-config/rules_provider.json');
const metadata =require('co-config/metadata-xpaths.json');
const truncateList = ['titre','titrefr','titreen'];
const idAlphabet = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_';
const scriptList = {
  "setIdChain":"ctx._source.idChain = params.idChain",
  "setIsDuplicate":"if (ctx._source.duplicate == null || ctx._source.duplicate.size() == 0 ) { ctx._source.isDuplicate = false } else { ctx._source.isDuplicate = true }",
  "addDuplicate":"if (ctx._source.duplicate == null || ctx._source.duplicate.length==0){ ctx._source.duplicate = params.duplicate } else { if (!ctx._source.duplicate.contains(params.duplicate[0])) {ctx._source.duplicate.add(params.duplicate[0])}} ",
  "removeDuplicate":"if ((ctx._source.duplicate != null && ctx._source.duplicate.length>0)){ for (int i=0;i<ctx._source.duplicate.length;i++){ if (ctx._source.duplicate[i].idConditor==params.idConditor){ ctx._source.duplicate.remove(i)}}}",
  "setDuplicateRules":"ArrayList mergedRules = new ArrayList(); for (int i=0;i<ctx._source.duplicate.length;i++) { for (int j = 0 ; j < ctx._source.duplicate[i].rules.length; j++){ if (!mergedRules.contains(ctx._source.duplicate[i].rules[j])) mergedRules.add(ctx._source.duplicate[i].rules[j]); }} mergedRules.sort(null); ctx._source.duplicateRules = mergedRules; "
};


const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: ['debug','error']
    }
});

const business = {};


function insertMetadata(docObject, options) {
    _.each(metadata, (metadatum) => {
        if (metadatum.indexed === undefined || metadatum.indexed === true) {
            if (docObject[metadatum.name] && docObject[metadatum.name].value && docObject[metadatum.name].value !== '') {
                options.body[metadatum.name] = { 'value': docObject[metadatum.name].value, 'normalized': docObject[metadatum.name].value };
                if (_.indexOf(truncateList,metadatum.name)!==-1) {
                    options.body[metadatum.name].normalized50 = docObject[metadatum.name].value;
                    options.body[metadatum.name].normalized75 = docObject[metadatum.name].value;
                }
            }
            else {
                options.body[metadatum.name] = docObject[metadatum.name];
            }
        }
    });
}

function insereNotice(docObject){

	
	let options = {index : esConf.index,type : esConf.type,refresh:true};

	debug(esConf);

	options.body= {
		'dateCreation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
	};

  insertMetadata(docObject, options);

  options.body.path = docObject.path;
  options.body.source = docObject.source;
  options.body.typeConditor = [];
  options.body.idConditor = docObject.idConditor;
  options.body.ingestId = docObject.ingestId;
  options.body.ingestBaseName = docObject.ingestBaseName; 
  options.body.isDeduplicable = docObject.isDeduplicable;
  _.each(docObject.typeConditor,(typeCond)=>{
    options.body.typeConditor.push({'value':typeCond.type,'raw':typeCond.type});
  });
  options.body.idChain = docObject.source+':'+docObject.idConditor;
  docObject.duplicate = [];
  docObject.isDuplicate = false;
  options.body.duplicate = docObject.duplicate;
  options.body.isDuplicate = docObject.isDuplicate;
  
  return esClient.index(options);

}



function aggregeNotice(docObject, data) {


    let duplicate=[];
    let allMergedRules=[];
    let idchain=[];
    let arrayIdConditor=[];
    let regexp = new RegExp('.*:(.*)','g');


    
    _.each(data.hits.hits,(hit)=>{
        duplicate.push({idConditor:hit._source.idConditor,rules:hit.matched_queries,rules_keyword:hit.matched_queries,idIngest:hit._source.idIngest});
        idchain=_.union(idchain,hit._source.idChain.split('!'));
        allMergedRules = _.union(hit.matched_queries, allMergedRules);
    });

    

    arrayIdConditor = _.map(idchain,(idConditor)=>{
        return idConditor.replace(regexp,'$1');
    });

    idchain.push(docObject.source+':'+docObject.idConditor);
    idchain.sort();

    docObject.duplicate = duplicate;
    docObject.duplicateRules = _.sortBy(allMergedRules);
    docObject.isDuplicate = (allMergedRules.length > 0);

    let options = {index : esConf.index,type : esConf.type,refresh:true};
    
    debug(esConf);

    options.body= {
        'dateCreation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
    };

    insertMetadata(docObject, options);

    options.body.path = docObject.path;
    options.body.source = docObject.source;
    options.body.duplicate = duplicate;
    options.body.duplicateRules = allMergedRules;
    options.body.isDuplicate = (allMergedRules.length > 0);
    options.body.typeConditor = [];
    options.body.idConditor = docObject.idConditor;
    options.body.ingestId = docObject.ingestId;
    options.body.ingestBaseName = docObject.ingestBaseName;
    options.body.isDeduplicable = docObject.isDeduplicable;
    _.each(docObject.typeConditor,(typeCond)=>{
        options.body.typeConditor.push({'value':typeCond.type,'raw':typeCond.type});
    });
    docObject.arrayIdConditor=arrayIdConditor;
    options.body.idChain = _.join(idchain,'!');
    docObject.idChain = options.body.idChain;
    return esClient.index(options);
}

function propagate(docObject,data,result){


    let options;
    let update;
    let body=[];
    let option;
    let arrayDuplicate;
    let allMatchedRules;
    let propagateRequest;
    let matched_queries;

    _.each(result.hits.hits,(hit)=>{
       
        matched_queries = undefined;
        
        _.each(docObject.duplicate,(directDuplicate)=>{
            if (directDuplicate.idConditor === hit._source.idConditor) { matched_queries = directDuplicate.rules; }
        });

        options={update:{_index:esConf.index,_type:esConf.type,_id:hit._id,retry_on_conflict:3}};
       
        if (matched_queries!==undefined){
            update={script:
                {lang:"painless",
                source:scriptList.addDuplicate,
                params:{duplicate:[{
                        idConditor:docObject.idConditor,
                        rules:matched_queries,
                        rules_keyword:matched_queries
                    }],
                }},refresh:true
            };
            body.push(options);
            body.push(update);

        }
        update={script:
            {lang:"painless",
            source:scriptList.setIdChain,
            params:{idChain:docObject.idChain}
            },refresh:true
        };

        body.push(options);
        body.push(update);

        update={script:
            {lang:"painless",
            source:scriptList.setIsDuplicate,
            },refresh:true
        };

        body.push(options);
        body.push(update);

        update={script:
            {lang:"painless",
            source:scriptList.setDuplicateRules,
            },refresh:true
        };

        body.push(options);
        body.push(update);

    });
    option={body:body};
    
    return esClient.bulk(option);
}

function getDuplicateByIdConditor(docObject,data,result){

    docObject.idElasticsearch = result._id;

    let request = _.cloneDeep(baseRequest);
    _.each(docObject.arrayIdConditor,(idConditor)=>{
        request.query.bool.should.push({"bool":{"must":[{"term":{"idConditor":idConditor}}]}});
    });
    
    request.query.bool.minimum_should_match = 1;

    return esClient.search({
        index:esConf.index,
        body:request
    });
}


function dispatch(docObject,data) {

    // creation de l'id
    docObject.idConditor = generate(idAlphabet,25);
    
    if (data.hits.total===0){
        //console.log('on insere');
        return insereNotice(docObject);
    }
    else {
        //console.log('on aggrege');
        return aggregeNotice(docObject,data)
                .then(getDuplicateByIdConditor.bind(null,docObject,data))
                .then(propagate.bind(null,docObject,data),(error)=>{
                    console.error(error);
        });
    }
}

function testParameter(docObject,rules){

    let arrayParameter = rules.non_empty;
    let arrayNonParameter = (rules.is_empty!==undefined) ? rules.is_empty : [];
    let bool=true;
    _.each(arrayParameter,function(parameter){
        if (_.get(docObject,parameter)===undefined || _.get(docObject,parameter).trim()===''){ bool = false ;}
    });
    _.each(arrayNonParameter,function(nonparameter){
        if (_.get(docObject,nonparameter)!==undefined && _.get(docObject,nonparameter).trim()!==''){ bool = false;}
    })
    return bool;
}

function interprete(docObject,query,type){
    
    let rulename;
    if (type.trim()!==''){
        rulename = type+' : '+query.bool._name;
    }
    else {
        rulename = query.bool._name;
    }

    const newQuery ={
        bool: {
            must:null,
            _name:rulename
    }};
    
    newQuery.bool.must =  _.map(query.bool.must,(value)=>{
        let match = {'term':null};
        match.term = _.mapValues(value.match,(pattern)=>{
            return _.get(docObject,pattern);
        });
        return match;
    });
   
    if (type!==''){
        newQuery.bool.must.push({'term':{'typeConditor.value':type}});
    }
    return newQuery;
  
}

function buildQuery(docObject,request){
    
    _.each(docObject.typeConditor, (type)=>{
        
        if (type && type.type && scenario[type.type]){
            _.each(rules,(rule)=>{
                if (_.indexOf(scenario[type.type],rule.rule)!==-1 && testParameter(docObject,rule)) {
                        request.query.bool.should.push(interprete(docObject,rule.query,type.type));
                    }
            });
        }
    });
    return request;
}

// on crée la requete puis on teste si l'entrée existe
function existNotice(docObject){
    
    return Promise.try(function(){
        let request = _.cloneDeep(baseRequest);
        let data;
        // construction des règles par scénarii
        request = buildQuery(docObject,request);

        if (request.query.bool.should.length===0){
            docObject.isDeduplicable = false;
            data = {'hits':{'total':0}};
            return dispatch(docObject,data);
        }
        else{
            
            docObject.isDeduplicable = true;
            // construction des règles uniquement sur l'identifiant de la source
            
            _.each(provider_rules,(provider_rule)=>{
                if (docObject.source.trim()===provider_rule.source.trim() && testParameter(docObject,provider_rule.non_empty)){
                    request.query.bool.should.push(interprete(docObject,provider_rule.query,''))
                }
            });
            

            return esClient.search({
                index: esConf.index,
                body : request
            }).then(dispatch.bind(null,docObject));
        }
    });

}

function deleteNotice(docObject,data){
    docObject.idConditor = data.hits.hits[0].idConditor;
    return esClient.delete({
        index:esConf.index,
        type :esConf.type,
        id:data.hits.hits[0]._id,
        refresh:true
    });

}


function getDuplicateByIdChain(docObject,data,result){
    let request = _.cloneDeep(baseRequest);
    request.query.bool.should.push({"bool":{"must":[{"term":{"idChain":data.hits.hits[0]._source.idChain}}]}});
    return esClient.search({
        index:esConf.index,
        body:request
    });
}

function propagateDelete(docObject,data,result){

    return Promise.try(function(){
        let options;
        let update;
        let body=[];
        let option;
        let arrayDuplicate=[];
        let arrayIdChain;
        let idChainModify;
        let regexp;
        let allMatchedRules=[];
        if (result.hits.total>0){

            regexp = new RegExp(''+docObject.source+':'+docObject.idConditor+'[!]*','g');
            idChainModify = result.hits.hits[0]._source.idChain.replace(regexp,'');

            _.each(result.hits.hits,(hit)=>{
            
               
                options={update:{_index:esConf.index,_type:esConf.type,_id:hit._id,retry_on_conflict:3}};
               
                update={script:
                    {lang:"painless",
                    source:scriptList.removeDuplicate,
                    params:{idConditor:docObject.idConditor}
                    },refresh:true
                };

                body.push(options);
                body.push(update);
        
                update={script:
                    {lang:"painless",
                    source:scriptList.setIdChain,
                    params:{idChain:idChainModify}
                    },refresh:true
                };
        
                body.push(options);
                body.push(update);
        
                update={script:
                    {lang:"painless",
                    source:scriptList.setIsDuplicate,
                    },refresh:true
                };
        
                body.push(options);
                body.push(update);
        
                update={script:
                    {lang:"painless",
                    source:scriptList.setDuplicateRules,
                    },refresh:true
                };
        
                body.push(options);
                body.push(update);

            });

            option={body:body};
            return esClient.bulk(option);
        }
    });
}


function erase(docObject,data){
    return Promise.try(function(){
        if (data.hits.total===0) {return;}
        else if (data.hits.total>=2) {throw new Error('Erreur de mise à jour de notice : ID source présent en plusieurs exemplaires'); }
        else {
            return deleteNotice(docObject,data)
                    .then(getDuplicateByIdChain.bind(null,docObject,data))
                    .then(propagateDelete.bind(null,docObject,data))
                    .catch(function(e){
                        throw new Error('Erreur de mise à jour de notice : '+e);
                    });
        }
    });
}


function cleanByIdSource(docObject){

    return Promise.try(function(){

        let request = _.cloneDeep(baseRequest);
        let request_source;

        _.each(provider_rules,(provider_rule)=>{
            if (docObject.source.trim()===provider_rule.source.trim() && testParameter(docObject,provider_rule.non_empty)){
                request.query.bool.should.push(interprete(docObject,provider_rule.query,''));
                request_source = {"bool": {
                                    "must":[
                                        {"term": {"source": docObject.source.trim()}}
                                    ],
                                    "_name":"provider"}};
            
                request.query.bool.should.push(request_source);
                request.query.bool.minimum_should_match = 2;
            }
        });

        
        if (request.query.bool.should.length===0) {
            return;
            //throw new Error('Erreur de dédoublonnage : identifiant source absent ');
        }
        
        return esClient.search({
            index: esConf.index,
            body : request
        }).then(erase.bind(null,docObject))
        

    });

}


business.doTheJob = function(docObject, cb) {

    let error;

    cleanByIdSource(docObject).then(existNotice.bind(null,docObject)).then(function(result) {

        //debug(result);
        //debug(docObject);
        if (result && result._id && !docObject.idElasticsearch) { docObject.idElasticsearch = result._id;}
        return cb();

    }).catch(function(e){
        error = {
            errCode: 3,
            errMessage: 'erreur de dédoublonnage : ' + e
        };
        docObject.error = error;
        cb(error);
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
