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
- **test_extract.py** : Tests unitaires pour le module d'extraction
- **test_transform.py** : Tests unitaires pour le module de transformation
- **test_load.py** : Tests unitaires pour le module de chargement
- **test_integration.py** : Tests d'intégration pour le pipeline complet

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