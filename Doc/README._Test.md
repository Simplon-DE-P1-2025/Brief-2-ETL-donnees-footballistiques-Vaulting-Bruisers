# World Cup ETL Pipeline

Projet ETL visant à centraliser, nettoyer et normaliser l’ensemble des matchs de la Coupe du Monde de la FIFA de 1930 à 2022 à partir de sources hétérogènes (CSV et JSON).

L’objectif est de produire un dataset unique, cohérent et directement exploitable pour des analyses ou des modèles futurs.

## Objectifs
Construire un pipeline ETL complet (Extract, Transform, Load)
Harmoniser les données issues de sources disparates
Normaliser les noms d’équipes, villes, tours et dates
Éliminer les doublons et incohérences
Charger les données dans une base relationnelle SQLite
Documenter et structurer le projet de manière claire

## Données traitées
1930 – 2010 : CSV (formats et encodages hétérogènes)
2014 : CSV enrichi (dates, stades)
2018 : JSON imbriqué (groupes, phases finales, stades)
2022 : CSV incomplet, enrichi via sources additionnelles

Volume total : ~7 700 matchs.

## Modèle de données
Les données finales sont stockées dans une table unique world_cup_matches, avec une ligne par match de Coupe du Monde, toutes éditions confondues.

## Champs principaux :
équipes (home / away)
scores
date
tour de compétition
ville
édition
métadonnées (stade, codes FIFA, source…)
Ce choix garantit un schéma simple, homogène et facilement exploitable qui répond aux attentes de rendu pour le brief.

## Choix techniques
Python pour l’ETL
SQLite comme base de données (léger, portable, suffisant pour le volume)
Pandas pour la manipulation des données
Unidecode pour la normalisation des noms (équipes, villes, stades)
Architecture claire Extract → Transform → Load

## Pipeline ETL
Le résultat est un fichier worldcup.db prêt pour l’analyse.

## Structure du projet
.
├── main.py
├── extract.py        # WorldCupExtractor
├── transform.py      # WorldCupTransformer
├── load.py           # WorldCupLoader
├── run_tests.py      # Script d'exécution des tests
├── data/
│   ├── raw/          # fichiers sources (non versionnés)
│   ├── processed/    # fichiers intermédiaires
│   └── Tests/        # Tests unitaires et d'intégration
│       ├── test_extract.py
│       ├── test_transform.py
│       ├── test_load.py
│       └── test_integration.py
└── README.md


Les sources de données ne sont pas versionnées volontairement.

## Tests
Le projet inclut une suite complète de tests pour assurer la qualité et la fiabilité du pipeline ETL.

### Exécution des tests
```bash
# Installation des dépendances de test
pip install -r requirements.txt

# Exécution de tous les tests
python run_tests.py

# Ou directement avec pytest
pytest data/Tests/ -v --cov=extract --cov=transform --cov=load
```

### Couverture des tests
Le projet dispose d'une couverture de test de **79%** avec **76 tests** couvrant tous les aspects critiques du pipeline ETL.

#### Tests par module

##### **test_extract.py** (89% de couverture)
Tests unitaires pour l'extraction de données depuis 4 sources hétérogènes :

**Source 1 (1930-2010)** :
- Extraction CSV standard avec gestion d'erreurs
- Test de fichier inexistant (exception FileNotFoundError)

**Source 2 (2014)** :
- Extraction CSV avec fallback encodage (UTF-8-SIG → Latin-1)
- Nettoyage des artefacts spécifiques au fichier
- Tests d'encodage avec fichiers temporaires Latin-1

**Source 3 (2022)** :
- Détection automatique du séparateur (virgule/point-virgule)
- Normalisation des en-têtes de colonnes
- Tests de fallback séparateur avec fichiers temporaires

**Source 4 (2018)** :
- Chargement JSON hiérarchique (groupes, phases finales, stades)
- Test de JSON invalide (exception JSONDecodeError)

**Sources auxiliaires** :
- Extraction dates historiques (TXT avec fallback encodage)
- Extraction villes 2022 (CSV avec séparateur ';')
- Tests de fichiers corrompus et gestion d'erreurs

##### **test_transform.py** (74% de couverture)
Tests unitaires pour la transformation et normalisation des données :

**Fonctions de base** :
- Parsing de scores (regex universelle, tuples, None, chaînes vides)
- Normalisation d'équipes (mappings, encodages, chiffres → "Unknown")
- Normalisation de villes (suppression parenthèses, mappings)
- Normalisation de rounds (Group Stage, Final, etc.)
- Calcul de résultats (home win, away win, draw, gestion None/NaN)
- Parsing de dates (formats multiples, gestion erreurs)

**Transformation par source** :
- **Source 1** : Parsing scores, normalisation équipes/villes/rounds, calcul résultats
- **Source 2** : Extraction colonnes spécifiques, gestion dates, cas limites (None)
- **Source 3** : Mapping colonnes dynamique, parsing dates spéciales, gestion erreurs
- **Source 4** : Extraction groupes/knockout, mapping stades, normalisation rounds

**Fonctions avancées** :
- Enrichissement dates historiques (appariement complexe multi-matchs)
- Enrichissement villes 2022 (lookup table)
- Consolidation (fusion, dédoublonnage, tri, gestion erreurs)
- Validation (complétude colonnes, logique scores)
- Analyse résultats (statistiques)

**Tests de robustesse** :
- Gestion d'erreurs (exceptions, None, NaN)
- Cas limites (fichiers vides, DataFrames None)
- Logique complexe (appariement dates multiples équipes)

##### **test_load.py** (100% de couverture)
Tests unitaires pour le chargement en base de données :

- Initialisation et connexion SQLite
- Création du schéma (table world_cup_matches)
- Chargement des données avec gestion transactions
- Vérification d'intégrité post-chargement
- Gestion d'erreurs (connexion invalide, rollback)
- Fermeture propre des connexions

##### **test_integration.py** (Tests d'intégration)
Tests de bout en bout du pipeline complet :

- **Pipeline complet** : Extract → Transform → Load → Base de données
- **ETL intégré** : Test des interactions entre modules
- Utilisation de données de test réalistes
- Vérification de la base finale et du CSV exporté

### Métriques de couverture
```
extract.py        96     11    89%   (lignes non couvertes : gestion d'erreurs externes)
load.py           48      0   100%   (couverture complète)
transform.py     416    107    74%   (lignes non couvertes : logique complexe, logs)
TOTAL            560    118    79%
```

### Types de tests
- **Tests unitaires** : Fonctions individuelles, cas nominaux et d'erreur
- **Tests d'intégration** : Pipeline complet avec données réalistes
- **Tests de robustesse** : Gestion d'erreurs, cas limites, encodages
- **Tests de performance** : Couverture élevée, exécution rapide (< 2s)

## Améliorations possibles
Gestion des versions via branches et pull requests
Automatisation des tests
Ajout de nouvelles éditions (ex. 2026)
Enrichissements statistiques supplémentaires

## Équipe
Dahani Fernando
Matthieu Navarro
David Brimeux
Mathieu Barbé-Gayet