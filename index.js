/* global module */
/*jslint node: true */
/*jslint indent: 2 */
"use strict";

var es = require('elasticsearch'),
	_ = require('lodash'),
	fs = require('fs');

var esConf = require('./es.js');
var esMapping = require('./mapping.json');

var esClient = new es.Client({
	host: esConf.host
});

var business = {};

// on teste si l'entrée existe
// si oui global alors on alimente le fichier de bulk en update
// si non global alors on alimente le fichier de bulk en création
// si oui partiel alors on met en quarantaine en attendant décision humaine

function existNotice(jsonLine){


	var found=false;

	esClient.search({
		query : {
			field : {
				titre: {normalized: jsonLine.titre.normalized},
				doi: {normalized: jsonLine.doi.normalized}

			}
		}
	},function(err,data){

		console.log('########################');
		console.log(err);
		console.log('########################');
		console.log(data)

		return found;
	});


}


business.doTheJob = function (jsonLine, cb) {

	try {

		jsonLine.bulk = [];

		var result = {},
			bulk = {
				_type: esConf._type,
			};

		if (existNotice(jsonLine)) {
			result = {
				update: bulk
			};
			result.update._index = jsonLine.elasticIndex;
		} else {
			result = {
				index: bulk
			};
		}

		jsonLine.bulk.push(result);
		jsonLine.bulk.push(fs.readFileSync(jsonLine.path, "utf8"));


	} catch(e) {

		var err = {};
		jsonLine.errCode = 1701;
		jsonLine.errMessage = 'Erreur lors de l\'ajout du champ bulk : '+e;
		return cb({
			errCode: jsonLine.errCode,
			errMessage:jsonLine.errMessage
		});

	}

	return cb();

}




// fonction préalable de création d'index si celui-ci absent.
// appelé dans beforeAnyJob

function createIndex(conditorSession,options,indexCallback){

	var reqParams = {
		_index:conditorSession
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

			esMapping.settings.index = {
				"number_of_replicas" : 0
			};
			reqParams.body = esMapping;

			esClient.indices.create(reqParams,function(err,response,status){
				console.log(JSON.stringify(reqParams));
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
					"index": 'conditor'
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

	var conditorSession = process.env.CONDITOR_SESSION || 'conditor';
	createIndex(conditorSession,options,function(err){
		options.errLogs.push("callback createIndex, err="+err);
		return cbBefore(err,options);
	});
}

// Fonction d'envoi du bulk
business.finalJob = function(docObjects, cb) {

	var options = {
		processLogs: [],
		errLogs: []
	};

	options.processLogs.push(`finalJob sur ${docObjects.length} documents`);

	if (docObjects.length && docObjects[0].elasticIndex) {
		var body = [];
		for (var i = 0; i < docObjects.length; i++) {
			for (var j = 0; j < docObjects[i].bulk.length; j++) {
				body.push((j === 0) ? docObjects[i].bulk[j] : { doc : JSON.parse(docObjects[i].bulk[j]) });
			}
		}
		esClient.bulk({
			body: body,
			refresh: "wait_for"
		}, function(err, resp) {
			cb(err);
		});
		return;
	}

	// On parcourt docObjects en retirant les champs bulk, ajoutés dans bulkBody
	var bulkBody = [],
		i;

	for (i = 0; i < docObjects.length; i++) {
		bulkBody = bulkBody.concat(_.cloneDeep(docObjects[i].bulk));
		_.unset(docObjects[i], 'bulk');
	}

	esClient.bulk({
		body: bulkBody
	}, function(err, response, status) {

		options.processLogs.push("Bulk fini avec le code "+status,err);

		var errDocObjects = [];

		if (err) {
			options.processLogs.push("err true dans retour de bulk callback "+status,err);
			options.errLogs.push('Erreur durant l\'indexation : ' + err + '\n');

			// On récupère alors tous les docObjects que l'on place dans errDocObjects
			errDocObjects = _.cloneDeep(docObjects);
			docObjects = [];

			// On ajoute le code erreur et message (sur le premier docObject, à voir si nécessaire de répliquer sur tous)
			errDocObjects[0].index.errCode = 1704;
			errDocObjects[0].index.errMessage = 'Erreur sur tout le fichier durant l\'indexation : ' + err + '\n';
			esClient.close();
			return cb(errDocObjects, options);

		} else {

			options.processLogs.push("err false dans retour de bulk callback "+status,err);

			if (response.errors) {
				options.processLogs.push("response.errors true dans retour de bulk callback "+status,err);
				response.items.forEach(function(index) {

					if (index.index.error) {

						options.errLogs.push('Index ' + index.index._id + ' en erreur : ' + index.index.error + '\n');
						var indexErrDoc = _.findIndex(docObjects, {
							idIstex: index.index._id
						});
						errDocObjects.push(_.cloneDeep(docObjects[indexErrDoc]));
						_.pullAt(docObjects, indexErrDoc);
						_.last(errDocObjects).errCode = 1705;
						_.last(errDocObjects).errMessage = 'Erreur durant l\'indexation : ' + index.index.error;
					}
				});

				options.processLogs.push('... Envoyé mais comporte une(des) erreur(s)\n');
				esClient.close();
				return cb(errDocObjects, options);

			} else {

				options.processLogs.push('... Bien envoyé\n');
				esClient.close();
				return cb(null, options);
			}
		}
	});
}

module.exports = business;