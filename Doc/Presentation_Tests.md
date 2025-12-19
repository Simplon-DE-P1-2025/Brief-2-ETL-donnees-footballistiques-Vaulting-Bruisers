# Présentation : Tests du Pipeline ETL World Cup

## 🎯 Objectif de cette présentation
Comprendre l'importance des tests dans un projet Data et découvrir comment nous avons testé notre pipeline ETL de données footballistiques.

---

## 📋 Qu'est-ce qu'un test en Data Engineering ?

### Pourquoi tester ?
- **Garantir la qualité** : S'assurer que le code fonctionne correctement
- **Prévenir les bugs** : Détecter les erreurs avant la production
- **Faciliter l'évolution** : Modifier le code en toute sécurité
- **Documenter le comportement** : Les tests expliquent ce que fait le code

### Types de tests dans notre projet
- **Tests unitaires** : Tester une fonction spécifique
- **Tests d'intégration** : Tester l'interaction entre modules
- **Tests de robustesse** : Tester la gestion d'erreurs

---

## 🏗️ Architecture de notre pipeline ETL

```
Sources de données → Extract → Transform → Load → Base de données
     ↓              ↓         ↓         ↓         ↓
   Tests          Tests     Tests     Tests     Tests
```

**4 sources de données** : CSV 1930-2010, CSV 2014, JSON 2018, CSV 2022

---

## 🧪 Tests d'Extraction (Extract)

### Objectif
Vérifier que nous pouvons lire correctement toutes les sources de données malgré leurs différences.

### Tests par source

#### 📄 Source 1 : CSV Classique (1930-2010)
```python
def test_extract_source1(self, temp_data_dir):
    extractor = WorldCupExtractor(data_dir=temp_data_dir)
    df = extractor.extract_source1("matches_19302010.csv")
    assert isinstance(df, pd.DataFrame)  # Vérifie que c'est un DataFrame
    assert len(df) == 2                 # Vérifie le nombre de lignes
    assert 'round' in df.columns        # Vérifie les colonnes attendues
```

**Ce que ça teste** : Lecture CSV standard, gestion des erreurs de fichier

#### 📄 Source 2 : CSV Complexe (2014)
```python
def test_extract_source2_encoding_fallback(self, temp_data_dir):
    # Test avec fichier encodé en Latin-1
    df = extractor.extract_source2("WorldCupMatches2014_latin.csv")
    assert isinstance(df, pd.DataFrame)
```

**Ce que ça teste** : Fallback automatique UTF-8 → Latin-1, nettoyage des données bruitées

#### 📄 Source 3 : CSV Flexible (2022)
```python
def test_extract_source3_fallback_separator(self, temp_data_dir):
    # Test avec fichier utilisant point-virgule
    df = extractor.extract_source3("Fifa_world_cup_matches_semicolon.csv")
    assert isinstance(df, pd.DataFrame)
```

**Ce que ça teste** : Détection automatique du séparateur, normalisation des noms de colonnes

#### 📄 Source 4 : JSON Hiérarchique (2018)
```python
def test_extract_source4(self, temp_data_dir):
    data = extractor.extract_source4("data_2018.json")
    assert isinstance(data, dict)      # Vérifie que c'est un dictionnaire
    assert 'groups' in data           # Vérifie la structure attendue
```

**Ce que ça teste** : Lecture JSON complexe avec groupes et phases finales

---

## 🔄 Tests de Transformation (Transform)

### Objectif
Vérifier que nous nettoyons et normalisons correctement les données.

### Tests de fonctions de base

#### 🎯 Parsing des scores
```python
def test_parse_score_valid(self, transformer):
    home, away = transformer.parse_score("2-1")
    assert home == 2 and away == 1

def test_parse_score_invalid(self, transformer):
    home, away = transformer.parse_score("invalid")
    assert home is None and away is None
```

**Ce que ça teste** : Regex universelle pour différents formats de scores

#### 🏆 Normalisation des équipes
```python
def test_normalize_team(self, transformer):
    assert transformer.normalize_team("West Germany") == "Germany"
    assert transformer.normalize_team("Brazil") == "Brazil"
```

**Ce que ça teste** : Mappings pour gérer les noms d'équipes variables

#### 🏟️ Normalisation des villes
```python
def test_normalize_city(self, transformer):
    assert transformer.normalize_city("PARIS") == "Paris"
    assert transformer.normalize_city("Rio de Janeiro") == "Rio De Janeiro"
```

**Ce que ça teste** : Nettoyage et standardisation des noms de villes

### Tests par source

#### Source 1 : Nettoyage complet
- Parsing des scores depuis la colonne "score"
- Normalisation équipes, villes, rounds
- Calcul automatique du résultat (home/away/draw)

#### Source 2 : Extraction ciblée
- Sélection des bonnes colonnes (Home Team Name, Away Team Goals, etc.)
- Gestion des dates au format "14 Jun 2014 - 13:00"

#### Source 3 : Mapping dynamique
- Détection automatique des colonnes de scores
- Parsing spécial des dates ("01 Jan" → 2022-01-01)

#### Source 4 : Structure complexe
- Extraction des matchs de groupes ET phases finales
- Mapping des IDs d'équipes vers noms réels
- Liaison avec les stades

---

## 💾 Tests de Chargement (Load)

### Objectif
Vérifier que nous sauvegardons correctement les données en base.

```python
def test_load_data(self, loader, sample_df):
    loader.connect()
    loader.create_schema()
    loader.load_data(sample_df)

    # Vérifier que les données sont bien en base
    df_loaded = pd.read_sql_query("SELECT * FROM world_cup_matches", loader.conn)
    assert len(df_loaded) == 2
    assert df_loaded.iloc[0]['home_team'] == 'Brazil'
```

**Ce que ça teste** :
- Connexion à SQLite
- Création de la table avec le bon schéma
- Insertion des données
- Gestion des transactions et rollback en cas d'erreur

---

## 🔗 Tests d'Intégration

### Objectif
Vérifier que tout le pipeline fonctionne ensemble.

```python
def test_full_pipeline_execution(self, temp_workspace):
    # Exécuter le pipeline complet
    run_etl_pipeline()

    # Vérifier les résultats
    db_path = Path("data/worldcup.db")
    assert db_path.exists()

    csv_path = Path("data/processed/worldcup_clean.csv")
    assert csv_path.exists()

    # Vérifier le contenu de la base
    conn = sqlite3.connect(str(db_path))
    df = pd.read_sql_query("SELECT * FROM world_cup_matches", conn)
    assert len(df) > 0
```

**Ce que ça teste** : Le pipeline complet de bout en bout

---

## 📊 Métriques de qualité

### Couverture des tests
- **extract.py**   : 89% 
- **transform.py** : 74% 
- **load.py**      : 100%
- **Total**        : 79%

### Nombre de tests
- **76 tests** au total
- Tests unitaires, d'intégration, de robustesse

---

## 🎓 Leçons apprises pour les Data Learners

### 1. **L'importance des tests**
- Un code non testé est un code risqué
- Les tests donnent confiance pour modifier le code
- Ils servent de documentation vivante

### 2. **Stratégie de test**
- Tester les cas nominaux (ça marche)
- Tester les cas d'erreur (ça plante proprement)
- Tester les cas limites (données manquantes, formats étranges)

### 3. **Outils utilisés**
- **pytest** : Framework de test Python
- **pandas** : Pour manipuler les DataFrames de test
- **sqlite3** : Pour tester la base de données
- **tempfile** : Pour créer des fichiers temporaires de test

### 4. **Bonnes pratiques**
- Un test = une fonctionnalité
- Noms descriptifs (test_extract_source1_file_not_found)
- Tests indépendants les uns des autres
- Utiliser des fixtures pour réutiliser le code

---

## 🚀 Pour aller plus loin

### Tests avancés à implémenter
- Tests de performance (vitesse d'exécution)
- Tests avec de gros volumes de données
- Tests d'API si on expose les données
- Tests automatisés dans un pipeline CI/CD

