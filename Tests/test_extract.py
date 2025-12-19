import pytest
import pandas as pd
import json
import tempfile
import os
from pathlib import Path
from extract import WorldCupExtractor
import logging

# Configuration du logging pour les tests
logging.basicConfig(level=logging.INFO)

class TestWorldCupExtractor:
    """Tests pour la classe WorldCupExtractor."""

    @pytest.fixture
    def extractor(self):
        """Fixture pour créer une instance de WorldCupExtractor."""
        return WorldCupExtractor(data_dir="data/raw")

    @pytest.fixture
    def temp_data_dir(self):
        """Fixture pour créer un répertoire temporaire avec des données de test."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Créer des fichiers de test temporaires
            temp_path = Path(temp_dir)

            # Fichier CSV source1
            df1 = pd.DataFrame({
                'round': ['Group A', 'Group B'],
                'team1': ['Brazil', 'Argentina'],
                'team2': ['France', 'Germany'],
                'score': ['2-1', '1-0'],
                'venue': ['Rio', 'Buenos Aires'],
                'year': [2014, 2014]
            })
            df1.to_csv(temp_path / "matches_19302010.csv", index=False)

            # Fichier CSV source2 (avec séparateur ;)
            df2 = pd.DataFrame({
                'Home Team Name': ['Brazil', 'Argentina'],
                'Away Team Name': ['France', 'Germany'],
                'Home Team Goals': [2, 1],
                'Away Team Goals': [1, 0],
                'City': ['Rio', 'Buenos Aires'],
                'Stage': ['Group A', 'Group B'],
                'Year': [2014, 2014],
                'Datetime': ['14 Jun 2014 - 13:00', '15 Jun 2014 - 16:00']
            })
            df2.to_csv(temp_path / "WorldCupMatches2014.csv", sep=';', index=False)

            # Fichier CSV source3
            df3 = pd.DataFrame({
                'team1': ['Brazil', 'Argentina'],
                'team2': ['France', 'Germany'],
                'number of goals team1': [2, 1],
                'number of goals team2': [1, 0],
                'city': ['Rio', 'Buenos Aires'],
                'round': ['Group A', 'Group B'],
                'year': [2022, 2022],
                'date': ['01 Jan', '02 Jan']
            })
            df3.to_csv(temp_path / "Fifa_world_cup_matches.csv", index=False)

            # Fichier JSON source4
            json_data = {
                "groups": {
                    "A": {
                        "matches": [
                            {
                                "home_team": 1,
                                "away_team": 2,
                                "home_result": 2,
                                "away_result": 1,
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
            with open(temp_path / "data_2018.json", 'w') as f:
                json.dump(json_data, f)

            # Fichier TXT dates historiques
            txt_content = "home_team,away_team,date_exacte\nBrazil,France,14/06/2014\nArgentina,Germany,15/06/2014\n"
            with open(temp_path / "dates_1930_2010.txt", 'w') as f:
                f.write(txt_content)

            # Fichier CSV cities 2022
            df_cities = pd.DataFrame({
                'home_team': ['Brazil'],
                'away_team': ['France'],
                'city': ['Rio']
            })
            df_cities.to_csv(temp_path / "cities_2022.csv", sep=';', index=False)

            yield temp_path

    def test_init(self):
        """Test de l'initialisation."""
        extractor = WorldCupExtractor()
        assert extractor.data_dir == Path("data/raw")

    def test_extract_source1(self, temp_data_dir):
        """Test extraction source 1."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_source1("matches_19302010.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'round' in df.columns

    def test_extract_source2(self, temp_data_dir):
        """Test extraction source 2."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_source2("WorldCupMatches2014.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'Home Team Name' in df.columns

    def test_extract_source3(self, temp_data_dir):
        """Test extraction source 3."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_source3("Fifa_world_cup_matches.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'team1' in df.columns

    def test_extract_source4(self, temp_data_dir):
        """Test extraction source 4."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        data = extractor.extract_source4("data_2018.json")
        assert isinstance(data, dict)
        assert 'groups' in data

    def test_extract_historical_dates(self, temp_data_dir):
        """Test extraction dates historiques."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_historical_dates("dates_1930_2010.txt")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'home_team' in df.columns

    def test_extract_cities_2022(self, temp_data_dir):
        """Test extraction villes 2022."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_cities_2022("cities_2022.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert 'city' in df.columns

    def test_extract_source1_file_not_found(self, extractor):
        """Test extraction source 1 avec fichier inexistant."""
        with pytest.raises(FileNotFoundError):
            extractor.extract_source1("nonexistent.csv")

    def test_extract_source4_invalid_json(self, temp_data_dir):
        """Test extraction source 4 avec JSON invalide."""
        # Créer un fichier JSON invalide
        invalid_path = temp_data_dir / "invalid.json"
        with open(invalid_path, 'w') as f:
            f.write("invalid json")

        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        with pytest.raises(json.JSONDecodeError):
            extractor.extract_source4("invalid.json")

    def test_extract_source2_encoding_fallback(self, temp_data_dir):
        """Test extraction source 2 avec fallback encodage."""
        # Créer un fichier avec encodage Latin-1
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
        path = temp_data_dir / "WorldCupMatches2014_latin.csv"
        df2.to_csv(path, sep=';', encoding='latin-1', index=False)

        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_source2("WorldCupMatches2014_latin.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_extract_historical_dates_encoding_fallback(self, temp_data_dir):
        """Test extraction dates historiques avec fallback encodage."""
        txt_content = "home_team,away_team,date_exacte\nGermany,Argentina,13/07/2014\n"
        path = temp_data_dir / "dates_latin.txt"
        with open(path, 'w', encoding='latin-1') as f:
            f.write(txt_content)

        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_historical_dates("dates_latin.txt")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_extract_source3_fallback_separator(self, temp_data_dir):
        """Test extraction source 3 avec fallback séparateur."""
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
        path = temp_data_dir / "Fifa_world_cup_matches_semicolon.csv"
        df3.to_csv(path, sep=';', index=False)

        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        df = extractor.extract_source3("Fifa_world_cup_matches_semicolon.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_extract_source2_encoding_fallback_complete(self, temp_data_dir, monkeypatch):
        """Test complet du fallback encodage dans extract_source2 avec mock."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)

        # Mock pd.read_csv pour lever UnicodeDecodeError sur UTF-8-sig
        call_count = [0]
        def mock_read_csv(filepath, sep, encoding):
            call_count[0] += 1
            if encoding == 'utf-8-sig' and call_count[0] == 1:
                raise UnicodeDecodeError('utf-8', b'', 0, 1, 'invalid')
            # Succès avec latin-1
            return pd.DataFrame({
                'Home Team Name': ['Brazil'],
                'Away Team Name': ['France'],
                'Home Team Goals': [2],
                'Away Team Goals': [1],
                'City': ['Rio'],
                'Stage': ['Group A'],
                'Year': [2014],
                'Datetime': ['14 Jun 2014 - 13:00']
            })

        monkeypatch.setattr('pandas.read_csv', mock_read_csv)
        df = extractor.extract_source2("dummy.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert call_count[0] == 2  # Deux appels : UTF-8-sig puis latin-1

    def test_extract_source3_separator_fallback_complete(self, temp_data_dir, monkeypatch):
        """Test complet du fallback séparateur dans extract_source3 avec mock."""
        extractor = WorldCupExtractor(data_dir=temp_data_dir)

        call_count = [0]
        def mock_read_csv(filepath, sep=None, encoding=None):
            call_count[0] += 1
            if sep is None and call_count[0] == 1:  # Premier appel sans sep
                raise Exception("Invalid separator")
            # Succès avec sep=';'
            return pd.DataFrame({
                'team1': ['Qatar'],
                'team2': ['Ecuador'],
                'number of goals team1': [0],
                'number of goals team2': [2],
                'city': ['Doha'],
                'round': ['Group A'],
                'year': [2022],
                'date': ['20 Nov']
            })

        monkeypatch.setattr('pandas.read_csv', mock_read_csv)
        df = extractor.extract_source3("dummy.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert call_count[0] == 2  # Deux appels : virgule puis point-virgule

    def test_extract_historical_dates_corrupted_file(self, temp_data_dir):
        """Test extraction dates historiques avec fichier corrompu."""
        # Créer un fichier TXT corrompu
        corrupted_path = temp_data_dir / "corrupted_dates.txt"
        with open(corrupted_path, 'w', encoding='utf-8') as f:
            f.write("invalid;content;here\nmore;invalid;data\n")

        extractor = WorldCupExtractor(data_dir=temp_data_dir)
        result = extractor.extract_historical_dates("corrupted_dates.txt")
        assert result is None  # Devrait retourner None en cas d'erreur