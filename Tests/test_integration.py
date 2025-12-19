import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from main import run_etl_pipeline
from extract import WorldCupExtractor
from transform import WorldCupTransformer
from load import WorldCupLoader
import logging

# Configuration du logging pour les tests
logging.basicConfig(level=logging.INFO)

class TestETLPipeline:
    """Tests d'intégration pour le pipeline ETL complet."""

    @pytest.fixture
    def temp_workspace(self):
        """Fixture pour créer un espace de travail temporaire."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Créer la structure de répertoires
            (temp_path / "data" / "raw").mkdir(parents=True)
            (temp_path / "data" / "processed").mkdir(parents=True)

            # Créer des fichiers de données de test minimaux
            # Source 1: matches_19302010.csv
            df1 = pd.DataFrame({
                'round': ['Group A'],
                'team1': ['Brazil'],
                'team2': ['France'],
                'score': ['2-1'],
                'venue': ['Rio'],
                'year': [2014]
            })
            df1.to_csv(temp_path / "data" / "raw" / "matches_19302010.csv", index=False)

            # Source 2: WorldCupMatches2014.csv
            df2 = pd.DataFrame({
                'Home Team Name': ['Germany'],
                'Away Team Name': ['Argentina'],
                'Home Team Goals': [1],
                'Away Team Goals': [0],
                'City': ['Berlin'],
                'Stage': ['Final'],
                'Year': [2014],
                'Datetime': ['13 Jul 2014 - 16:00']
            })
            df2.to_csv(temp_path / "data" / "raw" / "WorldCupMatches2014.csv", sep=';', index=False)

            # Source 3: Fifa_world_cup_matches.csv
            df3 = pd.DataFrame({
                'team1': ['Qatar'],
                'team2': ['Ecuador'],
                'number of goals team1': [0],
                'number of goals team2': [2],
                'city': ['Doha'],
                'round': ['Group A'],
                'year': [2022],
                'date': ['20 Nov']
            })
            df3.to_csv(temp_path / "data" / "raw" / "Fifa_world_cup_matches.csv", index=False)

            # Source 4: data_2018.json
            json_data = {
                "groups": {
                    "A": {
                        "matches": [
                            {
                                "home_team": 1,
                                "away_team": 2,
                                "home_result": 5,
                                "away_result": 0,
                                "date": "2018-06-14T18:00:00+03:00",
                                "stadium": 1
                            }
                        ]
                    }
                },
                "stadiums": [
                    {"id": 1, "city": "Moscow"}
                ]
            }
            import json
            with open(temp_path / "data" / "raw" / "data_2018.json", 'w') as f:
                json.dump(json_data, f)

            # Dates historiques: dates_1930_2010.txt
            txt_content = "home_team,away_team,date_exacte\nBrazil,France,12/06/2014\n"
            with open(temp_path / "data" / "raw" / "dates_1930_2010.txt", 'w') as f:
                f.write(txt_content)

            # Villes 2022: cities_2022.csv
            df_cities = pd.DataFrame({
                'home_team': ['Qatar'],
                'away_team': ['Ecuador'],
                'city': ['Doha']
            })
            df_cities.to_csv(temp_path / "data" / "raw" / "cities_2022.csv", sep=';', index=False)

            # Changer le répertoire de travail
            original_cwd = os.getcwd()
            os.chdir(temp_path)
            yield temp_path
            os.chdir(original_cwd)

    def test_full_pipeline_execution(self, temp_workspace):
        """Test d'exécution complète du pipeline ETL."""
        # Exécuter le pipeline
        run_etl_pipeline()

        # Vérifier que la base de données a été créée
        db_path = Path("data/worldcup.db")
        assert db_path.exists()

        # Vérifier que le fichier CSV a été créé
        csv_path = Path("data/processed/worldcup_clean.csv")
        assert csv_path.exists()

        # Vérifier le contenu de la base de données
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        df = pd.read_sql_query("SELECT * FROM world_cup_matches", conn)
        conn.close()

        assert len(df) > 0
        assert 'home_team' in df.columns
        assert 'away_team' in df.columns

    def test_extract_transform_load_integration(self, temp_workspace):
        """Test d'intégration des modules Extract, Transform, Load."""
        # Extraction
        extractor = WorldCupExtractor(data_dir="data/raw")
        df1 = extractor.extract_source1("matches_19302010.csv")
        df2 = extractor.extract_source2("WorldCupMatches2014.csv")
        df3 = extractor.extract_source3("Fifa_world_cup_matches.csv")
        json4 = extractor.extract_source4("data_2018.json")
        dates = extractor.extract_historical_dates("dates_1930_2010.txt")
        cities = extractor.extract_cities_2022("cities_2022.csv")

        # Transformation
        transformer = WorldCupTransformer()
        df_c1 = transformer.transform_source1(df1)
        if dates is not None:
            df_c1 = transformer.enrich_with_historical_dates(df_c1, dates)
        df_c2 = transformer.transform_source2(df2)
        df_c3 = transformer.transform_source3(df3)
        df_c4 = transformer.transform_source4(json4)

        if cities is not None:
            df_c3 = transformer.enrich_2022_with_cities(df_c3, cities)

        # Consolidation
        df_final = transformer.consolidate([df_c1, df_c2, df_c3, df_c4])

        # Chargement
        loader = WorldCupLoader(db_path="data/test_integration.db")
        loader.connect()
        loader.create_schema()
        loader.load_data(df_final)
        loader.verify_load()
        loader.close()

        # Vérifications
        assert df_final is not None
        assert len(df_final) > 0
        assert 'id_match' in df_final.columns

        # Vérifier la base de données
        import sqlite3
        conn = sqlite3.connect("data/test_integration.db")
        df_loaded = pd.read_sql_query("SELECT * FROM world_cup_matches", conn)
        conn.close()

        assert len(df_loaded) == len(df_final)