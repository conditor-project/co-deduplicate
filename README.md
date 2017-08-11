[![Build Status](https://travis-ci.org/conditor-project/co-deduplicate.svg?branch=master)](https://travis-ci.org/conditor-project/co-deduplicate)

co-deduplicate
===============

## Présentation ##

Le module **co-deduplicate** est un module qui va opérer le dédoublonnage de notices en passant par le moteur de recherche [Elasticsearch](https://www.elastic.co/fr/products/elasticsearch).


### Fonctionnement ###

`co-deduplicate` reçoit les notices préformatées au format JSON et interroge ElasticSearch selon une hierarchie de règles de correspondance prédéfinie. 

Si l'une des règles de correspondance est appliquée :

- l'objet documentaire est fusionné avec l'entrée existante :
  - si le champ "source" de l'entrée existante est identique à l'objet documentaire, alors ce dernier remplace le précédant
  - sinon, on conserve les infos issues des 2 notices
- si aucune règle ne s'applique, une nouvelle entrée est créée au sein du moteur, reprenant l'ensemble des champs de l'objet documentaire JSON

#### Structure d'entrée

Identique à la structure des champs de sortie du module [co-normalizer](https://github.com/conditor-project/co-normalizer#user-content-structure-de-sortie), elle contient pour chaque champ du chapeau Conditor :

- un sous-champ `value` contenant la valeur telle qu'elle est présente dans la notice d'origine
- un sous-champ`normalized` contenant la version *normalisée* (homogénéisée) du champ `value`

#### Structure de sortie

2 champs sont ajoutés au JSON d'entrée :

* `conditor_ident` : nombre entier permettant de connaître la règle de correspondance (vaut `99` si aucune règle ne s'applique)
* `id_elasticsearch` : identifiant de l'entrée du document dans le moteur Elasticsearch. 

#### Règles de dédoublonnage

Ces règles sont actuellement insérées directement dans le code de l'application (méthode `existNotice` du fichier [index.js](./index.js)). À l'avenir, elles pourraient être intégrées à un fichier de configuration.

Toutes les règles utilisent les version *normalisées* des différents champs, et si une entrée contient les informations issues de plusieurs sources, la correspondance sera tentée sur les valeurs de chaque source.

Les règles actuelles sont les suivantes :

1. correspondance sur `titre` ET `doi`
2. correspondance sur `titre`, `volume`, `numero` et `issn`
3. correspondance sur `doi` uniquement
4. correspondance sur `titre`, `auteur` et `issn`
5. correspondance sur `titre`, `auteur_init` et `issn`
6. correspondance sur `issn`, `volume`, `numero` et `page`


99. pas de correspondance

:warning: à l'avenir, ces règles seront pondérées, de manière à pouvoir identifier des doublons probables, mais qui demandent une validation manuelle.

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
