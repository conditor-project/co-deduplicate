[![Build Status](https://travis-ci.org/conditor-project/co-deduplicate.svg?branch=master)](https://travis-ci.org/conditor-project/co-deduplicate)

co-deduplicate
===============

## Présentation ##

Le module **co-deduplicate** est un module qui va opérer le dédoublonnage de notices en passant par le moteur de recherche [Elasticsearch](https://www.elastic.co/fr/products/elasticsearch).


### Fonctionnement ###

`co-deduplicate` reçoit les notices préformatées au format JSON et interroge ElasticSearch selon une hierarchie de règles de correspondance prédéfinie. 

Lorsque pour un document, un ou plusieurs doublons sont trouvés, l'objet documentaire au format JSON est enrichi avec les identifiants des documents doublons. Pour chaque doublon, la liste des règles effectuant la correspondance est mémorisée.

#### Structure d'entrée

Identique à la structure des champs de sortie du module [co-formatter](https://github.com/conditor-project/co-formatter), elle contient pour chaque champ du chapeau Conditor un sous-champ `value` contenant la valeur telle qu'elle est présente dans la notice d'origine.

#### Structure de sortie

2 champs sont ajoutés au JSON d'entrée :

* `idChain` : la liste des doublons trouvés, sous la forme d'une suite d'identifiants séparés par un caractère délimiteur.
* `duplicates` : la liste des noms de règles ayant amené au dédoublonnage

#### Normalisation

Les champs utilisés pour le dédoublonnage ne sont désormais plus normalisés en amont :  **le module [co-normalizer](https://github.com/conditor-project/co-normalizer) est à présent obsolète.**

Ces champs sont maintenant normalisés **à l'indexation**, grâce aux fonctionnalités d'analyse de texte du moteur de recherche Elasticsearch. Sont notamment utilisés les [normalizers](https://www.elastic.co/guide/en/elasticsearch/reference/6.0/analysis-normalizers.html), les [token filters](https://www.elastic.co/guide/en/elasticsearch/reference/6.0/analysis-tokenfilters.html) et les [character filters](https://www.elastic.co/guide/en/elasticsearch/reference/6.0/analysis-charfilters.html). Pour plus de détails, consulter le [mapping](./mapping.json) utilisé par le présent module.

Il résulte de ce mécanisme, que les **valeurs normalisées de tous ces champs ne sont pas stockées en clair,** mais seulement prises en compte lors de l'interrogation.

Si vous souhaitez tester les différents filtres, il convient d'interroger Elasticsearch directement. 

Prenons en  exemple la normalisation d'un titre, qui est spécifié ainsi dans le mapping  :

```json
"title:normalizer":{
  "type": "custom",
  "char_filter":["whitespace_remove","punctuation_remove"],
  "filter":["lowercase","my_asciifolding"]
}
```

Pour le simuler la normalisation d'un titre, il faudra donc envoyer la requête à Elasticsearch la requête suivante :

```json
POST /records/_analyze
{
  "tokenizer": "keyword",
  "char_filter":["whitespace_remove","punctuation_remove"],
  "filter":["lowercase","my_asciifolding"],
  "text": "c'est un bien joli titre que voilà !"
}
```

#### Règles de dédoublonnage

Ces règles sont numérotées, nommées et externalisées dans le fichier de configuration [rules_certain.json](./rules_certain.json). Ces règles sont ensuite exécuté selon des [scénarios prédéfinis](./scenario.json) en fonction du type de document.

:warning: à l'avenir, ces règles seront probablement pondérées, de manière à pouvoir identifier des doublons probables, mais qui demandent une validation manuelle.

## Utilisation ##

### Installation ###

Dépendances système : 
    * NodeJS 4.0.0+
    * ElasticSearch 5.4.0+

Commande d'installation :
```bash 
npm install 
```

### Vérification du fonctionnement ###
Commande d'éxécution des tests unitaires :
```bash 
npm test
```

### Exécution ###

Comme pour tous les modules, la présente partie métier n'est pas destinée à être exécutée directement, puisqu'elle consiste uniquement à mettre à disposition une fonction `doTheJob`.

L'exécution se fera donc en appelant cette fonction depuis une instanciation d`li-canvas` ou indirectement depuis les tests unitaires.

## Annexes ##

### Arborescence ###

```
.
├── index.js                        // Point d'entrée, contenant la fonction doTheJob()
├── node_modules                    // Modules NPM
│   ├── ...
├── package.json                    // No comment
├── README.md
└── test                            // Fichiers nécessaires aux TU
    ├── dataset                     // rép de données de tests
    │   └── in
    |       └── test.json          // contient 2 docObjects pris en entrée des TU
    ├── run.js                      // point d'entrée des TU
    └──
```
