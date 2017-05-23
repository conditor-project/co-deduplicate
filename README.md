co-undoubler
===============

## Présentation ##

Le module **co-undoubler** est un module qui va opérer le dédoublonnage de notices en passant par elasticsearch.


### Fonctionnement ###

`co-undoubler` reçoit les notices préformatées sous format JSON et interroge ElasticSearch selon une hierarchie de 
dédoublonnage prédéfinie afin de créer la notice en cas de non matching, de l'aggréger si matching, ou de renvoyer la 
notice en attente en cas de doute. 

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