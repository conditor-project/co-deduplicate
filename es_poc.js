"use strict";

// =================================================================
//	Déclaration des paramètres de connexion
// =================================================================

var elasticUrl = process.env.ELASTIC_URL || "http://localhost:9200";

module.exports = {
	host: elasticUrl,
	index:"test_poc",
	type: "notice_poc"
};