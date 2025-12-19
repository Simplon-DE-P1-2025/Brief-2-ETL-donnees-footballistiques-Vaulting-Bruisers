import pytest
import pandas as pd
from transform import WorldCupTransformer
import logging

# Configuration du logging pour les tests
logging.basicConfig(level=logging.INFO)

class TestWorldCupTransformer:
    """Tests pour la classe WorldCupTransformer."""

    @pytest.fixture
    def transformer(self):
        """Fixture pour créer une instance de WorldCupTransformer."""
        return WorldCupTransformer()

    @pytest.fixture
    def sample_df_source1(self):
        """DataFrame d'exemple pour source 1."""
        return pd.DataFrame({
            'round': ['Group A', 'Final'],
            'team1': ['Brazil', 'Germany'],
            'team2': ['France', 'Argentina'],
            'score': ['2-1', '1-0'],
            'venue': ['Rio', 'Berlin'],
            'year': [2010, 2010]
        })

    @pytest.fixture
    def sample_df_source2(self):
        """DataFrame d'exemple pour source 2."""
        return pd.DataFrame({
            'Home Team Name': ['Brazil', 'Germany'],
            'Away Team Name': ['France', 'Argentina'],
            'Home Team Goals': [2, 1],
            'Away Team Goals': [1, 0],
            'City': ['Rio', 'Berlin'],
            'Stage': ['Group A', 'Final'],
            'Year': [2014, 2014],
            'Datetime': ['14 Jun 2014 - 13:00', '15 Jun 2014 - 16:00']
        })

    @pytest.fixture
    def sample_df_source3(self):
        """DataFrame d'exemple pour source 3."""
        return pd.DataFrame({
            'team1': ['Brazil', 'Germany'],
            'team2': ['France', 'Argentina'],
            'number of goals team1': [2, 1],
            'number of goals team2': [1, 0],
            'city': ['Rio', 'Berlin'],
            'round': ['Group A', 'Final'],
            'year': [2022, 2022],
            'date': ['01 Jan', '02 Jan']
        })

    @pytest.fixture
    def sample_json_source4(self):
        """Données JSON d'exemple pour source 4."""
        return {
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

    def test_init(self):
        """Test de l'initialisation."""
        transformer = WorldCupTransformer()
        assert transformer.teams_mapping is not None
        assert transformer.cities_mapping is not None

    def test_parse_score_valid(self, transformer):
        """Test parsing de scores valides."""
        home, away = transformer.parse_score("2-1")
        assert home == 2
        assert away == 1

    def test_parse_score_invalid(self, transformer):
        """Test parsing de scores invalides."""
        home, away = transformer.parse_score("invalid")
        assert home is None
        assert away is None

    def test_normalize_team(self, transformer):
        """Test normalisation des équipes."""
        assert transformer.normalize_team("West Germany") == "Germany"
        assert transformer.normalize_team("Brazil") == "Brazil"
        assert transformer.normalize_team("") == ""

    def test_normalize_city(self, transformer):
        """Test normalisation des villes."""
        assert transformer.normalize_city("PARIS") == "Paris"
        assert transformer.normalize_city("Rio de Janeiro") == "Rio De Janeiro"

    def test_normalize_round(self, transformer):
        """Test normalisation des rounds."""
        assert transformer.normalize_round("Group A") == "Group Stage"
        assert transformer.normalize_round("Final") == "Final"

    def test_compute_result_home_win(self, transformer):
        """Test calcul du résultat - victoire domicile."""
        result = transformer.compute_result(2, 1, "Brazil", "France")
        assert result == "Brazil"

    def test_compute_result_away_win(self, transformer):
        """Test calcul du résultat - victoire extérieur."""
        result = transformer.compute_result(1, 2, "Brazil", "France")
        assert result == "France"

    def test_compute_result_draw(self, transformer):
        """Test calcul du résultat - match nul."""
        result = transformer.compute_result(1, 1, "Brazil", "France")
        assert result == "draw"

    def test_parse_datetime(self, transformer):
        """Test parsing des dates."""
        date = transformer.parse_datetime("14 Jun 2014")
        assert pd.notna(date)

    def test_transform_source1(self, transformer, sample_df_source1):
        """Test transformation source 1."""
        result = transformer.transform_source1(sample_df_source1)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'home_team' in result.columns
        assert 'away_team' in result.columns
        assert 'home_result' in result.columns

    def test_transform_source2(self, transformer, sample_df_source2):
        """Test transformation source 2."""
        result = transformer.transform_source2(sample_df_source2)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'home_team' in result.columns

    def test_transform_source3(self, transformer, sample_df_source3):
        """Test transformation source 3."""
        result = transformer.transform_source3(sample_df_source3)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'home_team' in result.columns

    def test_transform_source4(self, transformer, sample_json_source4):
        """Test transformation source 4."""
        result = transformer.transform_source4(sample_json_source4)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'home_team' in result.columns

    def test_consolidate(self, transformer):
        """Test consolidation des DataFrames."""
        df1 = pd.DataFrame({
            'home_team': ['Brazil'],
            'away_team': ['France'],
            'home_result': [2],
            'away_result': [1],
            'result': ['Brazil'],
            'date': [pd.to_datetime('2014-06-14')],
            'round': ['Group Stage'],
            'city': ['Rio'],
            'edition': ['2014']
        })
        df2 = pd.DataFrame({
            'home_team': ['Germany'],
            'away_team': ['Argentina'],
            'home_result': [1],
            'away_result': [0],
            'result': ['Germany'],
            'date': [pd.to_datetime('2014-06-15')],
            'round': ['Final'],
            'city': ['Berlin'],
            'edition': ['2014']
        })

        result = transformer.consolidate([df1, df2])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'id_match' in result.columns

    def test_validate(self, transformer):
        """Test validation des données."""
        df = pd.DataFrame({
            'id_match': [1, 2],
            'home_team': ['Brazil', 'Germany'],
            'away_team': ['France', 'Argentina'],
            'home_result': [2, 1],
            'away_result': [1, 0],
            'result': ['Brazil', 'Germany'],
            'date': [pd.to_datetime('2014-06-14'), pd.to_datetime('2014-06-15')],
            'round': ['Group Stage', 'Final'],
            'city': ['Rio', 'Berlin'],
            'edition': ['2014', '2014']
        })

        is_valid = transformer.validate(df)
        assert is_valid is True

    def test_analyze_results(self, transformer):
        """Test analyse des résultats."""
        df = pd.DataFrame({
            'result': ['Brazil', 'draw', 'France']
        })

        # Ne devrait pas lever d'exception
        transformer.analyze_results(df)

    def test_enrich_with_historical_dates(self, transformer):
        """Test enrichissement avec dates historiques."""
        df_matches = pd.DataFrame({
            'home_team': ['Brazil', 'Germany'],
            'away_team': ['France', 'Argentina'],
            'home_result': [2, 1],
            'away_result': [1, 0],
            'result': ['Brazil', 'Germany'],
            'date': [pd.to_datetime('2014-07-01'), pd.to_datetime('2014-07-01')],
            'round': ['Group Stage', 'Final'],
            'city': ['Rio', 'Berlin'],
            'edition': ['2014', '2014']
        })

        df_dates = pd.DataFrame({
            'home_team': ['Brazil'],
            'away_team': ['France'],
            'date_exacte': [pd.to_datetime('2014-06-14')]
        })

        result = transformer.enrich_with_historical_dates(df_matches, df_dates)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_enrich_with_historical_dates_none(self, transformer):
        """Test enrichissement avec dates None."""
        df_matches = pd.DataFrame({
            'home_team': ['Brazil'],
            'away_team': ['France'],
            'home_result': [2],
            'away_result': [1],
            'result': ['Brazil'],
            'date': [pd.to_datetime('2014-07-01')],
            'round': ['Group Stage'],
            'city': ['Rio'],
            'edition': ['2014']
        })

        result = transformer.enrich_with_historical_dates(df_matches, None)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_enrich_2022_with_cities(self, transformer):
        """Test enrichissement villes 2022."""
        df_2022 = pd.DataFrame({
            'home_team': ['Qatar'],
            'away_team': ['Ecuador'],
            'home_result': [0],
            'away_result': [2],
            'result': ['Ecuador'],
            'date': [pd.to_datetime('2022-11-20')],
            'round': ['Group A'],
            'city': ['Unknown'],
            'edition': ['2022']
        })

        df_cities = pd.DataFrame({
            'home_team': ['Qatar'],
            'away_team': ['Ecuador'],
            'city': ['Doha']
        })

        result = transformer.enrich_2022_with_cities(df_2022, df_cities)
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]['city'] == 'Doha'

    def test_enrich_2022_with_cities_none(self, transformer):
        """Test enrichissement villes 2022 avec None."""
        df_2022 = pd.DataFrame({
            'home_team': ['Qatar'],
            'away_team': ['Ecuador'],
            'home_result': [0],
            'away_result': [2],
            'result': ['Ecuador'],
            'date': [pd.to_datetime('2022-11-20')],
            'round': ['Group A'],
            'city': ['Unknown'],
            'edition': ['2022']
        })

        result = transformer.enrich_2022_with_cities(df_2022, None)
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]['city'] == 'Unknown'

    def test_parse_score_tuple(self, transformer):
        """Test parsing score avec tuple."""
        home, away = transformer.parse_score((2, 1))
        assert home == 2
        assert away == 1

    def test_parse_score_none(self, transformer):
        """Test parsing score avec None."""
        home, away = transformer.parse_score(None)
        assert home is None
        assert away is None

    def test_parse_score_empty_string(self, transformer):
        """Test parsing score avec chaîne vide."""
        home, away = transformer.parse_score('')
        assert home is None
        assert away is None

    def test_normalize_team_digit(self, transformer):
        """Test normalisation équipe avec chiffres."""
        result = transformer.normalize_team('123')
        assert result == 'Unknown'

    def test_normalize_team_na(self, transformer):
        """Test normalisation équipe avec NaN."""
        import numpy as np
        result = transformer.normalize_team(np.nan)
        assert result == 'Unknown'

    def test_normalize_city_na(self, transformer):
        """Test normalisation ville avec NaN."""
        import numpy as np
        result = transformer.normalize_city(np.nan)
        assert result is None

    def test_normalize_round_na(self, transformer):
        """Test normalisation round avec NaN."""
        import numpy as np
        result = transformer.normalize_round(np.nan)
        assert result is None

    def test_compute_result_na_home(self, transformer):
        """Test calcul résultat avec home_result NaN."""
        import numpy as np
        result = transformer.compute_result(np.nan, 1, 'Brazil', 'France')
        assert result is None

    def test_compute_result_na_away(self, transformer):
        """Test calcul résultat avec away_result NaN."""
        import numpy as np
        result = transformer.compute_result(2, np.nan, 'Brazil', 'France')
        assert result is None

    def test_compute_result_invalid_int(self, transformer):
        """Test calcul résultat avec valeurs non entières."""
        result = transformer.compute_result('invalid', 1, 'Brazil', 'France')
        assert result is None

    def test_parse_datetime_na(self, transformer):
        """Test parsing datetime avec NaN."""
        import numpy as np
        result = transformer.parse_datetime(np.nan)
        assert result is None

    def test_consolidate_empty_list(self, transformer):
        """Test consolidation avec liste vide."""
        result = transformer.consolidate([])
        assert result is None

    def test_consolidate_none_df(self, transformer):
        """Test consolidation avec DataFrame None."""
        result = transformer.consolidate([None])
        assert result is None

    def test_consolidate_empty_df(self, transformer):
        """Test consolidation avec DataFrame vide."""
        empty_df = pd.DataFrame()
        result = transformer.consolidate([empty_df])
        assert result is None

    def test_validate_inconsistent_result(self, transformer):
        """Test validation avec résultat incohérent."""
        df = pd.DataFrame({
            'id_match': [1],
            'home_team': ['Brazil'],
            'away_team': ['France'],
            'home_result': [1],
            'away_result': [2],
            'result': ['Brazil'],  # Incohérent car Brazil gagne mais score 1-2
            'date': [pd.to_datetime('2014-06-14')],
            'round': ['Group Stage'],
            'city': ['Rio'],
            'edition': ['2014']
        })

        is_valid = transformer.validate(df)
        assert is_valid is True  # La méthode retourne toujours True, juste log les erreurs

    def test_enrich_with_historical_dates_multiple_matches(self, transformer):
        """Test enrichissement avec plusieurs matchs entre mêmes équipes."""
        df_matches = pd.DataFrame({
            'home_team': ['Brazil', 'Brazil', 'Germany'],
            'away_team': ['France', 'France', 'Argentina'],
            'home_result': [2, 3, 1],
            'away_result': [1, 0, 0],
            'result': ['Brazil', 'Brazil', 'Germany'],
            'date': [pd.to_datetime('2014-07-01'), pd.to_datetime('2014-07-01'), pd.to_datetime('2014-07-01')],
            'round': ['Group Stage', 'Quarter-finals', 'Final'],
            'city': ['Rio', 'Rio', 'Berlin'],
            'edition': ['2014', '2014', '2014']
        })

        df_dates = pd.DataFrame({
            'home_team': ['Brazil', 'Brazil'],
            'away_team': ['France', 'France'],
            'date_exacte': [pd.to_datetime('2014-06-14'), pd.to_datetime('2014-06-28')]
        })

        result = transformer.enrich_with_historical_dates(df_matches, df_dates)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_parse_score_regex_cases(self, transformer):
        """Test parsing de scores avec différentes expressions régulières."""
        # Test avec différents formats
        test_cases = [
            ("2-1", (2, 1)),
            ("3:2", (3, 2)),
            ("1 - 0", (1, 0)),
            ("0:0", (0, 0)),
        ]

        for score_str, expected in test_cases:
            home, away = transformer.parse_score(score_str)
            assert home == expected[0]
            assert away == expected[1]

    def test_normalize_team_variations(self, transformer):
        """Test normalisation avec variations de noms."""
        variations = [
            ("FRANCE", "France"),
            ("BRAZIL", "Brazil"),
            ("United States", "USA"),
            ("Côte d'Ivoire", "Cote d'Ivoire"),
            ("Ivory Coast", "Cote d'Ivoire"),
        ]

        for input_name, expected in variations:
            result = transformer.normalize_team(input_name)
            assert result == expected