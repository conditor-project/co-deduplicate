'use strict';

const es = require('elasticsearch'),
    _ = require('lodash'),
    debug = require('debug')('co-deduplicate');

const Promise = require('bluebird');
const generate = require('nanoid/generate');
const unidecode = require('unidecode');
const fse = require('fs-extra');
const path = require('path');
const esConf = require('co-config/es.js');
//let esMapping = require('./mapping-shingles.json');
let esMapping = require('co-config/mapping-shingles.json');

    
const scenario = require('co-config/scenario.json');
//const scenario = require('./scenario_newname_suppression.json');
const rules = require('co-config/rules_certain.json');
//const rules = require('./rules_perline_newname_suppression_indent.json');
const baseRequest = require('co-config/base_request.json');
const provider_rules = require('co-config/rules_provider.json');
//const provider_rules = require('./rules_provider.json');
const metadata =require('co-config/metadata-xpaths.json');
const truncateList = ['title','titlefr','titleen'];
const idAlphabet = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_';
const scriptList = {
  "setIdChain":"ArrayList mergedId = new ArrayList(); mergedId.add(ctx._source.source+':'+ctx._source.idConditor+'!');String idChain ='!'; for (int i=0;i<ctx._source.duplicate.length;i++){mergedId.add(ctx._source.duplicate[i].source+':'+ctx._source.duplicate[i].idConditor+'!') } mergedId.sort(null);for (int j = 0; j<mergedId.length ; j++){ idChain+= mergedId[j] } ctx._source.idChain = idChain ",
  "setIsDuplicate":"if (ctx._source.duplicate == null || ctx._source.duplicate.length == 0 ) { ctx._source.isDuplicate = false } else { ctx._source.isDuplicate = true }",
  "addDuplicate":"if (ctx._source.duplicate == null || ctx._source.duplicate.length==0){ ctx._source.duplicate = params.duplicate } else { if (!ctx._source.duplicate.contains(params.duplicate[0])) {ctx._source.duplicate.add(params.duplicate[0])}} ",
  "removeDuplicate":"ArrayList newDuplicate = new ArrayList() ; if (ctx._source.duplicate != null && ctx._source.duplicate.length>0){int length = ctx._source.duplicate.length; for (int i=0;i<length;i++){ if (ctx._source.duplicate[i].idConditor!=params.idConditor){ newDuplicate.add(ctx._source.duplicate[i])}}} ctx._source.duplicate=newDuplicate;",
  "setDuplicateRules":"ArrayList mergedRules = new ArrayList(); for (int i=0;i<ctx._source.duplicate.length;i++) { for (int j = 0 ; j < ctx._source.duplicate[i].rules.length; j++){ if (!mergedRules.contains(ctx._source.duplicate[i].rules[j])) mergedRules.add(ctx._source.duplicate[i].rules[j]); }} mergedRules.sort(null); ctx._source.duplicateRules = mergedRules; ",
  "addEmptyDuplicate":"boolean present=false;for (int i=0;i<ctx._source.duplicate.length;i++){ if ( ctx._source.duplicate[i].idConditor == params.idConditor ) { present = true}} if ( !present ){ ctx._source.duplicate.add(params.duplicate[0]) } ",
  "setHadTransDuplicate":"boolean hadTransDuplicate=false;for (int i=0;i<ctx._source.duplicate.length;i++){ if (ctx._source.duplicate[i].rules.length==0){ hadTransDuplicate = true }} ctx._source.hadTransDuplicate = hadTransDuplicate "
};


const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: ['error']
    }
});

const business = {};


function insertMetadata(docObject,options){
    _.each(metadata,(metadatum)=>{
        if (metadatum.indexed === undefined || metadatum.indexed === true){
            if (_.isArray(docObject[metadatum.name])){
                options.body[metadatum.name] = docObject[metadatum.name];
            }
            else if (!_.isEmpty(docObject[metadatum.name])){
                options.body[metadatum.name] = docObject[metadatum.name];
            }
        }
    });
}

function insereNotice(docObject){

    return Promise.try(()=>{
	
        let options = {index : esConf.index,type : esConf.type,refresh:true};

        debug(esConf);

        options.body= {
            'creationDate': new Date().toISOString().replace(/T/,' ').replace(/\..+/,'')
        };

        
        insertMetadata(docObject,options);
        

        options.body.path = docObject.path;
        options.body.source = docObject.source;
        options.body.typeConditor = [];
        options.body.idConditor = docObject.idConditor;
        options.body.ingestId = docObject.ingestId;
        options.body.ingestBaseName = docObject.ingestBaseName; 
        options.body.isDeduplicable = docObject.isDeduplicable;

        
        _.each(docObject.typeConditor,(typeCond)=>{
            options.body.typeConditor.push(typeCond.type);
        });
       
       
        options.body.idChain = docObject.source+':'+docObject.idConditor+'!';
        docObject.duplicate = [];
        docObject.isDuplicate = false;
        options.body.duplicate = docObject.duplicate;
        options.body.isDuplicate = docObject.isDuplicate;
        //console.dir(options,10);
        //console.log('insertion : '+options.body.idHal.value);
        return esClient.index(options);
    });
}



function aggregeNotice(docObject, data) {

    return Promise.try(()=>{
        let duplicate=[];
        let allMergedRules=[];
        let idchain=[];
        let arrayIdConditor=[];
        let regexp = new RegExp('.*:(.*)','g');


        _.each(data.hits.hits,(hit)=>{

            if (hit._source.idConditor!==docObject.idConditor){
                duplicate.push({rules:hit.matched_queries,source:hit._source.source,ingestId:hit._source.ingestId,idConditor:hit._source.idConditor});
                idchain=_.union(idchain,hit._source.idChain.split('!'));
                allMergedRules = _.union(hit.matched_queries, allMergedRules);
            }
        });

        _.compact(idchain);

        arrayIdConditor = _.map(idchain,(idConditor)=>{
            return idConditor.replace(regexp,'$1');
        });

        idchain = _.map(idchain,(idConditor)=>{
            return idConditor+'!';
        });

        idchain.push(docObject.source+':'+docObject.idConditor+'!');
        idchain.sort();

        docObject.duplicate = duplicate;
        docObject.duplicateRules = _.sortBy(allMergedRules);
        docObject.isDuplicate = (allMergedRules.length > 0);

        let options = {index : esConf.index,type : esConf.type,refresh:true};
        
        debug(esConf);

        options.body= {
            'creationDate': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
        };

        
        insertMetadata(docObject,options);
        

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
            options.body.typeConditor.push(typeCond.type);
        });
        

        docObject.arrayIdConditor=arrayIdConditor;
        options.body.idChain =_.join(idchain,'');
        docObject.idChain = options.body.idChain;
        //console.log('insertion :'+JSON.stringify(options.body.idHal));
        return esClient.index(options);
    })
}

function propagate(docObject,data,result){

    let options;
    let update;
    let body=[];
    let option;
    let matched_queries;

    //console.log('nombre de resultat à la recherche par idConditor :'+result.hits.total);
    // On crée une liaison par défaut entre tous les duplicats trouvés
    _.each(result.hits.hits,(hit_target)=>{
        options={update:{_index:esConf.index,_type:esConf.type,_id:hit_target._id},retry_on_conflict:3};
        _.each(result.hits.hits,(hit_source)=>{
            if (hit_target._source.idConditor!==hit_source._source.idConditor){
                
                update={script:
                    {lang:"painless",
                    source:scriptList.addEmptyDuplicate,
                    params:{duplicate:[{
                        idConditor:hit_source._source.idConditor,
                        rules:[],
                        ingestId:hit_source._source.ingestId,
                        source: hit_source._source.source}],
                        idConditor:hit_source._source.idConditor
                    }},
                    refresh:true
                    
                };
                
                body.push(options);
                body.push(update);

            }
        });
    });

    _.each(result.hits.hits,(hit)=>{

        matched_queries = [];
        
        _.each(docObject.duplicate,(directDuplicate)=>{
            if (directDuplicate.idConditor === hit._source.idConditor) { matched_queries = directDuplicate.rules; }
        });

        options={update:{_index:esConf.index,_type:esConf.type,_id:hit._id,retry_on_conflict:3}};
        
        update={script:
            {lang:"painless",
            source:scriptList.removeDuplicate,
            params:{idConditor:docObject.idConditor}
            },refresh:true
        };

        body.push(options);
        body.push(update);


        if (hit._source.idConditor !== docObject.idConditor){
           
            update={script:
                {lang:"painless",
                source:scriptList.addDuplicate,
                params:{duplicate:[{
                        idConditor:docObject.idConditor,
                        rules:matched_queries,
                        ingestId:docObject.ingestId,
                        source: docObject.source
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

        update={script:
            {lang:"painless",
            source:scriptList.setHadTransDuplicate,
            },refresh:true
        };

        body.push(options);
        body.push(update);

    });

    options={update:{_index:esConf.index,_type:esConf.type,_id:docObject.idElasticsearch,retry_on_conflict:3}};
    update={script:
        {lang:"painless",
        source:scriptList.setHadTransDuplicate,
        },refresh:true
    };

    body.push(options);
    body.push(update);

    option={body:body};
    
    return esClient.bulk(option);
}

function getDuplicateByIdConditor(docObject,data,result){
        
    docObject.idElasticsearch = result._id;
    //console.log('recherche par id conditor  : ');
    let request = _.cloneDeep(baseRequest);
    _.each(docObject.arrayIdConditor,(idConditor)=>{
        if (idConditor.trim()!==""){
            request.query.bool.should.push({"bool":{"must":[{"term":{"idConditor":idConditor}}]}});
            //console.log(idConditor);
        }
    });
    
    request.query.bool.minimum_should_match = 1;

    return esClient.search({
        index:esConf.index,
        body:request
    });
   
}

function inspectResult(result){
    return Promise.try(()=>{
        console.log(result);
        return;
    });
}


function dispatch(docObject,data) {
    return Promise.try(()=>{
        // creation de l'id
        if (docObject.idConditor ===undefined ){ docObject.idConditor = generate(idAlphabet,25);}
        
        if (data.hits.total===0){
            
            return insereNotice(docObject).catch(function(err){
                if (err){  throw new Error('Erreur d insertion de notice: '+err);}
            });
        }
        else {
            
            return aggregeNotice(docObject,data)
                    .then(getDuplicateByIdConditor.bind(null,docObject,data))
                    .then(propagate.bind(null,docObject,data))
                    //.then(inspectResult)
                    .catch((err)=>{
                        if (err) { throw new Error('Erreur d aggregation de notice: '+err);}
                    });
                    
        }
    });
}

function testParameter(docObject,rules){

    let arrayParameter = (rules.non_empty!==undefined) ? rules.non_empty : [];
    let arrayNonParameter = (rules.is_empty!==undefined) ? rules.is_empty : [];
    let bool=true;
    _.each(arrayParameter,function(parameter){
        if (_.get(docObject,parameter)===undefined || 
        (_.isArray(_.get(docObject,parameter) && _.get(docObject,parameter).length === 0)) ||
        (_.isString(_.get(docObject,parameter)) && _.get(docObject,parameter).trim()===''))
        { bool = false ;}
    });
    
    _.each(arrayNonParameter,function(nonparameter){
        if (_.get(docObject,nonparameter)!==undefined && 
        ((_.isArray(_.get(docObject,nonparameter)) && _.get(docObject,nonparameter).length > 0) ||
        (_.isString(_.get(docObject,nonparameter)) && _.get(docObject,nonparameter).trim()!=='') 
            ))
         { bool = false;}
    })
    
    return bool;
}

function interprete(docObject,rule,type){
    
    let is_empty = (rule.is_empty!==undefined) ? rule.is_empty : [];
    let query = rule.query;
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
        let term,match,bool;
        
        if (value.match && _.isString(_.get(docObject,_.values(value.match)[0]))){
            match = {'match':null};
            match.match = _.mapValues(value.match,(pattern)=>{
                return unidecode(_.get(docObject,pattern));
            });
            return match;
        }
        else if (value.term && _.isString(_.get(docObject,_.values(value.term)[0]))){
            term = {'term':null};
            term.term = _.mapValues(value.term,(pattern)=>{
                return unidecode(_.get(docObject,pattern));
            });
            return term;
        }
        else if (value.match && _.isArray(_.get(docObject,_.values(value.match)[0]))){
            bool = {'bool':{}};
            bool.bool.should = _.map(_.get(docObject,_.values(value.match)[0]),(testValue)=>{
                let shouldMatch;
                shouldMatch={'match':{}};
                shouldMatch.match[_.values(value.match)[0]] = unidecode(testValue); 
                return shouldMatch;
            });
            bool.bool.minimum_should_match = 1;
            return bool;
        }
        else if (value.term && _.isArray(_.get(docObject,_.values(value.term)[0]))){
            bool = {'bool':{}};
            bool.bool.should = _.map(_.get(docObject,_.values(value.term)[0]),(testValue)=>{
                let shouldMatch;
                shouldMatch={'term':{}};
                shouldMatch.match[_.values(value.term)[0]] = unidecode(testValue);
                return shouldMatch;
            });
            bool.bool.minimum_should_match = 1;
            return bool;
        }
        else if (value.bool){
            bool = {'bool':{}};
            bool.bool.should = _.map(value.bool.should,(shouldCond)=>{
                let shouldTerm,shouldMatch;
                if (shouldCond.match && _.isString(_.get(docObject,_.values(shouldCond.match)[0]))){
                    shouldMatch = {'match':null};
                    shouldMatch.match = _.mapValues(shouldCond.match,(pattern)=>{
                        return unidecode(_.get(docObject,pattern));
                    });
                    return shouldMatch;
                }
                else if (shouldCond.term && _.isString(_.get(docObject,_.values(shouldCond.term)[0]))){
                    shouldTerm = {'term':null};
                    shouldTerm.term = _.mapValues(shouldCond.term,(pattern)=>{
                        return unidecode(_.get(docObject,pattern));
                    });
                    return shouldTerm;
                }
            });
            bool.bool.minimum_should_match = 1;
            return bool;
        }
    });
    
    //ajout de la précision que les champs doivent exister dans Elasticsearch
    
    if (is_empty.length>0) { newQuery.bool.must_not=[]}

    _.each(is_empty,(field)=>{
       newQuery.bool.must_not.push({'exists':{"field":field+".normalized"}});
    });
    if (type!==''){
        
        newQuery.bool.must.push({'match':{'typeConditor.normalized':type}});
       
    }
    return newQuery;
  
}

function buildQuery(docObject,request){
    
    _.each(docObject.typeConditor, (type)=>{
        
        if (type && type.type && scenario[type.type]){
            _.each(rules,(rule)=>{
                if (_.indexOf(scenario[type.type],rule.rule)!==-1 && testParameter(docObject,rule)) {
                        request.query.bool.should.push(interprete(docObject,rule,type.type));
                    }
            });
        }
    });
    return request;
}

// on crée la requete puis on teste si l'entrée existe
function existNotice(docObject){
    
    return Promise.try(()=>{
        let request = _.cloneDeep(baseRequest);
        let data;
        // construction des règles par scénarii
        request = buildQuery(docObject,request);

        //docObject.query_utile = request;

        if (request.query.bool.should.length===0){
            docObject.isDeduplicable = false;
            data = {'hits':{'total':0}};
            return dispatch(docObject,data);
        }
        else{
            
            docObject.isDeduplicable = true;
            // construction des règles uniquement sur l'identifiant de la source
            /**
            _.each(provider_rules,(provider_rule)=>{
                if (docObject.source.trim()===provider_rule.source.trim() && testParameter(docObject,provider_rule.non_empty)){
                    request.query.bool.should.push(interprete(docObject,provider_rule.query,''))
                }
            });
            **/
            //console.log(JSON.stringify(request));
            return esClient.search({
                index: esConf.index,
                body : request
            }).then(dispatch.bind(null,docObject));
        }
    });

}

function deleteNotice(docObject,data){
    docObject.idConditor = data.hits.hits[0]._source.idConditor;
    return esClient.delete({
        index:esConf.index,
        type :esConf.type,
        id:data.hits.hits[0]._id,
        refresh:true
    });
}


function getDuplicateByIdChain(docObject,data,result){
    
    //console.log('recherche par idChain : ');
    if (data.hits.hits[0]._source.isDuplicate){
        //console.log('c est un duplicat');
        //console.log(data.hits.hits[0]._source.idChain);
        let request = _.cloneDeep(baseRequest);
        //console.log('idChain:'+data.hits.hits[0]._source.idChain);
        request.query.bool.should.push({"bool":{"must":[{"match":{"idChain":data.hits.hits[0]._source.idChain}}]}});
        request.query.bool.minimum_should_match = 1;
        return esClient.search({
            index:esConf.index,
            body:request
        });
    }
    else {
        let answer = {'hits':{'total':0}};

        return Promise.try(()=>{
            return answer;
        });
    }
}

function propagateDelete(docObject,data,result){

    let options;
    let update;
    let body=[];
    let option;
    let arrayDuplicate=[];
    let arrayIdChain;
    let idChainModify;
    let regexp;
    let allMatchedRules=[];
    //console.log('resultat de l identification par idChain :');
    //console.log(result.hits.total);
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
                source:scriptList.setIdChain
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
   
}


function erase(docObject,data){
    return Promise.try(()=>{
        //console.log('DATA:'+JSON.stringify(data));
        if (data.hits.total>=2) {
            //console.log('erreur de mise à jour de notice');
            throw new Error('Erreur de mise à jour de notice : ID source présent en plusieurs exemplaires'); 
        }
        else if (data.hits.total===1){
            
            return deleteNotice(docObject,data)
                .then(getDuplicateByIdChain.bind(null,docObject,data))
                .then(propagateDelete.bind(null,docObject,data))
                .catch(function(e){
                    throw new Error('Erreur de mise à jour de notice : '+e);
                });
        }
    });
}


function getByIdSource(docObject){

    let request = _.cloneDeep(baseRequest);
    let request_source;
    let data;

    _.each(provider_rules,(provider_rule)=>{
        if (docObject.source.trim()===provider_rule.source.trim() && testParameter(docObject,provider_rule)){
            request.query.bool.should.push(interprete(docObject,provider_rule,''));
            request_source = {"bool": {
                                "must":[
                                    {"term": {"source": docObject.source.trim()}}
                                ],
                                "_name":"provider"}};
        
            request.query.bool.should.push(request_source);
            request.query.bool.minimum_should_match = 2;
        }
    });
    
    //console.log(JSON.stringify(request));

    if (request.query.bool.should.length===0) {
        
        data = {'hits':{'total':0}};

        return Promise.try(()=>{
            return data;
        });
        
        //throw new Error('Erreur de dédoublonnage : identifiant source absent ');
    }
    else {
        return esClient.search({
            index: esConf.index,
            body : request
        });
    }
}


business.doTheJob = function(docObject, cb) {

    let error;

    getByIdSource(docObject)
    .then(erase.bind(this,docObject))
    .then(existNotice.bind(this,docObject))
    .then(function(result) {
    
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


business.finalJob = function(docObject,cbFinal){

    esClient.indices.forcemerge()
    .catch(err=>{
        cbFinal(err);
    })
    .then(()=>{
        cbFinal();
    });
}

function createSnapshot(){
    return esClient.snapshot.create({
        "repository":esConf.index,
        "snapshot":"backup_"+esConf.index+new Date().toLocaleString().replace(' ','_').replace(/\..+/,''),
        "body":{
            "type":"fs",
            "settings":{
                "location":path.join(esConf.backup_path,esConf.index)
            }
        }
    });
}

function getRepository(){
    return esClient.snapshot.getRepository({
        "repository":esConf.index,
        "ignore":[404]
    });
}

function createRepository(response){
    if (response.status===404){
        return esClient.snapshot.createRepository({
            "repository":esConf.index,
            "body":{
                "type":"fs",
                "settings":{
                    "location":path.join(esConf.backup_path,esConf.index)
                }
            }
        })
        .catch(err=>{
            throw new Error('Erreur en creation de repository: '+err);
        });
    }
    else {
        Promise.try(()=>{
            return true;
        });
    }
}

business.afterAllTheJobs=function(cbAfterAll){
    getRepository()
    .then(createRepository)
    .catch(err=>{
        //console.log(err);
        cbAfterAll(err);
    })
    .then(createSnapshot)
    .catch(err=>{
        //console.log(err);
        cbAfterAll(err);
    })
    .then((response)=>{
        //console.log(response);
        cbAfterAll();
    });
}


module.exports = business;
