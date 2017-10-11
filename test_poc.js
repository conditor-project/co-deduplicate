/*jslint node: true */
/*jslint indent: 2 */
'use strict';

const es = require('elasticsearch'),
    _ = require('lodash'),
    fs = require('fs'),
    debug = require('debug')('co-deduplicate');

const esConf = require('./es.js');
const esMapping = require('./mapping_poc.json');

const esClient = new es.Client({
    host: esConf.host,
    log: {
        type: 'file',
        level: 'trace'
    }
});



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


            esMapping.settings = {
                'index': {
                    'number_of_replicas': 0
                }
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
                    name: 'integration_poc',
                    body: { 'actions': { 'add': { 'index': esConf.index, 'alias': 'integration_poc' } } }
                }, options, function(err) {
                    indexCallback(err);
                });

            });

        } else {
            indexCallback();
        }
    });
}

 function beforeAnyJob(cbBefore) {
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