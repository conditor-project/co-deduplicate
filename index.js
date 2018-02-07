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
  "removeDuplicate":"if ((ctx._source.duplicate != null && ctx._source.duplicate.length>0)){ for (int i=0;i<ctx._source.duplicate.length;i++){ if (ctx._source.duplicate[i].idConditor==params.idConditor){ ctx.source.duplicate.remove(i)}}}",
  "setDuplicateRules":"ArrayList mergedRules = new ArrayList(); for (int i=0;i<ctx._source.duplicate.length;i++) { for (int j = 0 ; j < ctx._source.duplicate[i].rules.length; j++){ if (!mergedRules.contains(ctx._source.duplicate[i].rules[j])) mergedRules.add(ctx._source.duplicate[i].rules[j]); }} mergedRules.sort(null); ctx._source.duplicateRules = mergedRules; "
};


const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: 'error'
    }
});

const business = {};


function insertMetadata(jsonLine, options) {
    _.each(metadata, (metadatum) => {
        if (metadatum.indexed === undefined || metadatum.indexed === true) {
            if (jsonLine[metadatum.name] && jsonLine[metadatum.name].value && jsonLine[metadatum.name].value !== '') {
                options.body[metadatum.name] = { 'value': jsonLine[metadatum.name].value, 'normalized': jsonLine[metadatum.name].value };
                if (_.indexOf(truncateList,metadatum.name)!==-1) {
                    options.body[metadatum.name].normalized50 = jsonLine[metadatum.name].value;
                    options.body[metadatum.name].normalized75 = jsonLine[metadatum.name].value;
                }
            }
            else {
                options.body[metadatum.name] = jsonLine[metadatum.name];
            }
        }
    });
}

function insereNotice(jsonLine){

	
	let options = {index : esConf.index,type : esConf.type,refresh:true};

	debug(esConf);

	options.body= {
		'dateCreation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
	};

  insertMetadata(jsonLine, options);

  options.body.path = jsonLine.path;
  options.body.source = jsonLine.source;
  options.body.typeConditor = [];
  options.body.idConditor = jsonLine.idConditor;
  options.body.ingestId = jsonLine.ingestId;
  options.body.ingestBaseName = jsonLine.ingestBaseName; 
  options.body.isDeduplicable = jsonLine.isDeduplicable;
  _.each(jsonLine.typeConditor,(typeCond)=>{
    options.body.typeConditor.push({'value':typeCond.type,'raw':typeCond.type});
  });
  options.body.idChain = jsonLine.source+':'+jsonLine.idConditor;
  options.body.duplicate = [];
  options.body.isDuplicate = false;
  jsonLine.duplicate = [];
  jsonLine.isDuplicate = false;
  return esClient.index(options);

}



function aggregeNotice(jsonLine, data) {


    let duplicate=[];
    let allMergedRules=[];
    let idchain=[];

    idchain.push(jsonLine.source+':'+jsonLine.idConditor);
    _.each(data.hits.hits,(hit)=>{
        duplicate.push({idConditor:hit._source.idConditor,rules:hit.matched_queries,rules_keyword:hit.matched_queries,idIngest:hit._source.idIngest});
        idchain=_.union(idchain,hit._source.idChain.split('!'));
        allMergedRules = _.union(hit.matched_queries, allMergedRules);
    });

    idchain.sort();
    jsonLine.duplicate = duplicate;
    jsonLine.duplicateRules = _.sortBy(allMergedRules);
    jsonLine.isDuplicate = (allMergedRules.length > 0);

    let options = {index : esConf.index,type : esConf.type,refresh:true};
    
    debug(esConf);

    options.body= {
        'dateCreation': new Date().toISOString().replace(/T/,' ').replace(/\..+/,''),
    };

    insertMetadata(jsonLine, options);

    options.body.path = jsonLine.path;
    options.body.source = jsonLine.source;
    options.body.duplicate = duplicate;
    options.body.duplicateRules = allMergedRules;
    options.body.isDuplicate = (allMergedRules.length > 0);
    options.body.typeConditor = [];
    options.body.idConditor = jsonLine.idConditor;
    options.body.ingestId = jsonLine.ingestId;
    options.body.ingestBaseName = jsonLine.ingestBaseName;
    options.body.isDeduplicable = jsonLine.isDeduplicable;
    _.each(jsonLine.typeConditor,(typeCond)=>{
        options.body.typeConditor.push({'value':typeCond.type,'raw':typeCond.type});
    });
    options.body.idChain = _.join(idchain,'!');
    jsonLine.idChain = options.body.idChain;
    return esClient.index(options);
}

function propagate(jsonLine,data,result){


    let options;
    let update;
    let body=[];
    let option;
    let arrayDuplicate;
    let allMatchedRules;

    jsonLine.idElasticsearch = result._id;

    _.each(data.hits.hits,(hit)=>{
       
        options={update:{_index:esConf.index,_type:esConf.type,_id:hit._id,retry_on_conflict:3}};
       
        update={script:
            {lang:"painless",
            inline:scriptList.addDuplicate,
            params:{duplicate:[{
                    idConditor:jsonLine.idConditor,
                    rules:hit.matched_queries,
                    rules_keyword:hit.matched_queries
                }],
            }}
        };
        body.push(options);
        body.push(update);

        update={script:
            {lang:"painless",
            inline:scriptList.setIdChain,
            params:{idChain:jsonLine.idChain}
            }
        };

        body.push(options);
        body.push(update);

        update={script:
            {lang:"painless",
            inline:scriptList.setIsDuplicate,
            }
        };

        body.push(options);
        body.push(update);

        update={script:
            {lang:"painless",
            inline:scriptList.setDuplicateRules,
            }
        };

        body.push(options);
        body.push(update);

    });
    option={body:body};
    
    return esClient.bulk(option);
}

function dispatch(jsonLine,data) {

    // creation de l'id
    jsonLine.idConditor = generate(idAlphabet,25);
    
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

function testParameter(jsonLine,rules){

    let arrayParameter = rules.non_empty;
    let arrayNonParameter = (rules.is_empty!==undefined) ? rules.is_empty : [];
    let bool=true;
    _.each(arrayParameter,function(parameter){
        if (_.get(jsonLine,parameter)===undefined || _.get(jsonLine,parameter).trim()===''){ bool = false ;}
    });
    _.each(arrayNonParameter,function(nonparameter){
        if (_.get(jsonLine,nonparameter)!==undefined && _.get(jsonLine,nonparameter).trim()!==''){ bool = false;}
    })
    return bool;
}

function interprete(jsonLine,query,type){
    
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
        let match = {'match':null};
        match.match = _.mapValues(value.match,(pattern)=>{
            return _.get(jsonLine,pattern);
        });
        return match;
    });
   
    if (type!==''){
        newQuery.bool.must.push({'match':{'typeConditor.value':type}});
    }
    return newQuery;
  
}

function buildQuery(jsonLine,request){
    
    _.each(jsonLine.typeConditor, (type)=>{
        
        if (type && type.type && scenario[type.type]){
            _.each(rules,(rule)=>{
                if (_.indexOf(scenario[type.type],rule.rule)!==-1 && testParameter(jsonLine,rule)) {
                        request.query.bool.should.push(interprete(jsonLine,rule.query,type.type));
                    }
            });
        }
    });
    return request;
}

// on crée la requete puis on teste si l'entrée existe
function existNotice(jsonLine){
    
    return Promise.try(function(){
        let request = _.cloneDeep(baseRequest);
        let data;
        // construction des règles par scénarii
        request = buildQuery(jsonLine,request);

        if (request.query.bool.should.length===0){
            jsonLine.isDeduplicable = false;
            data = {'hits':{'total':0}};
            return dispatch(jsonLine,data);
        }
        else{
            
            jsonLine.isDeduplicable = true;
            // construction des règles uniquement sur l'identifiant de la source
            
            _.each(provider_rules,(provider_rule)=>{
                if (jsonLine.source.trim()===provider_rule.source.trim() && testParameter(jsonLine,provider_rule.non_empty)){
                    request.query.bool.should.push(interprete(jsonLine,provider_rule.query,''))
                }
            });
            

            return esClient.search({
                index: esConf.index,
                body : request
            }).then(dispatch.bind(null,jsonLine));
        }
    });

}

function deleteNotice(jsonLine,data){
    jsonLine.idConditor = data.hits.hits[0].idConditor;
    return esClient.delete({
        index:esConf.index,
        type :esConf.type,
        id:data.hits.hits[0]._id,
        refresh:true
    });

}

function getDuplicate(jsonLine,data,result){
    let request = _.cloneDeep(baseRequest);
    request.query.bool.should.push({"bool":{"must":[{"match":{"idChain":data.hits.hits[0]._source.idChain}}]}});
    return esClient.search({
        index:esConf.index,
        body:request
    });
}

function propagateDelete(jsonLine,data,result){

    return Promise.try(function(){
        let options;
        let update;
        let body=[];
        let option;
        let arrayDuplicate=[];
        let idChainModify;
        let regexp;
        let allMatchedRules=[];
        if (result.hits.total>0){
            _.each(result.hits.hits,(hit)=>{
            
                regexp = new RegExp(''+hit._source.source+':'+hit._source.idConditor+'[!]*','g');
                idChainModify = hit._source.idChain.replace(regexp,'');
                options={update:{_index:esConf.index,_type:esConf.type,_id:hit._id,retry_on_conflict:3}};
               
                update={script:
                    {lang:"painless",
                    inline:scriptList.removeDuplicate,
                    params:{idConditor:jsonLine.idConditor,
                    }}
                };

                body.push(options);
                body.push(update);
        
                update={script:
                    {lang:"painless",
                    inline:scriptList.setIdChain,
                    params:{idChain:idChainModify}
                    }
                };
        
                body.push(options);
                body.push(update);
        
                update={script:
                    {lang:"painless",
                    inline:scriptList.setIsDuplicate,
                    }
                };
        
                body.push(options);
                body.push(update);
        
                update={script:
                    {lang:"painless",
                    inline:scriptList.setDuplicateRules,
                    }
                };
        
                body.push(options);
                body.push(update);

            });

            option={body:body};
            return esClient.bulk(option);
        }
    });
}


function erase(jsonLine,data){
    return Promise.try(function(){
        if (data.hits.total===0) {return;}
        else {
            return deleteNotice(jsonLine,data)
                    .then(getDuplicate.bind(null,jsonLine,data))
                    .then(propagateDelete.bind(null,jsonLine,data))
                    .catch(function(e){
                        throw new Error('Erreur de mise à jour de notice : '+e);
                    });
        }
    });
}


function cleanByIdSource(jsonLine){

    return Promise.try(function(){

        let request = _.cloneDeep(baseRequest);
        let request_source;

        _.each(provider_rules,(provider_rule)=>{
            if (jsonLine.source.trim()===provider_rule.source.trim() && testParameter(jsonLine,provider_rule.non_empty)){
                request.query.bool.should.push(interprete(jsonLine,provider_rule.query,''));
                request_source = {"bool": {
                                    "must":[
                                        {"match": {"source": jsonLine.source.trim()}}
                                    ],
                                    "_name":"provider"}};
            
                request.query.bool.should.push(request_source);
                request.query.bool.minimum_should_match = 2;
            }
        });

        if (request.query.bool.should.length===0) {
            return ;
        }
        
        return esClient.search({
            index: esConf.index,
            body : request
        }).then(erase.bind(null,jsonLine))

    });

}

business.doTheJob = function(jsonLine, cb) {

    let error;

    cleanByIdSource(jsonLine).then(existNotice.bind(null,jsonLine)).then(function(result) {

        //debug(result);
        //debug(jsonLine);
        if (result && result._id && !jsonLine.idElasticsearch) { jsonLine.idElasticsearch = result._id;}
        return cb();

    }).catch(function(e){
        error = {
            errCode: 3,
            errMessage: 'erreur de dédoublonnage : ' + e
        };
        jsonLine.error = error;
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
