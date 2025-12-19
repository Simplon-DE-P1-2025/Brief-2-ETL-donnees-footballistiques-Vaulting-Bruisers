import pytest
import pandas as pd
import sqlite3
import os
from load import WorldCupLoader
import logging

# Configuration du logging pour les tests
logging.basicConfig(level=logging.INFO)

class TestWorldCupLoader:
    """Tests pour la classe WorldCupLoader."""

    @pytest.fixture
    def temp_db_path(self):
        """Fixture pour créer un chemin de base de données temporaire."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        yield db_path
        # Nettoyage
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def loader(self, temp_db_path):
        """Fixture pour créer une instance de WorldCupLoader."""
        return WorldCupLoader(db_path=temp_db_path)

    @pytest.fixture
    def sample_df(self):
        """DataFrame d'exemple pour les tests."""
        return pd.DataFrame({
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

    def test_init(self, temp_db_path):
        """Test de l'initialisation."""
        loader = WorldCupLoader(db_path=temp_db_path)
        assert loader.db_path == temp_db_path
        assert loader.conn is None

    def test_connect(self, loader):
        """Test de la connexion à la base de données."""
        loader.connect()
        assert loader.conn is not None
        assert isinstance(loader.conn, sqlite3.Connection)
        loader.close()

    def test_create_schema(self, loader):
        """Test de la création du schéma."""
        loader.connect()
        loader.create_schema()

        # Vérifier que la table existe
        cursor = loader.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='world_cup_matches'")
        tables = cursor.fetchall()
        assert len(tables) == 1

        # Vérifier les colonnes
        cursor.execute("PRAGMA table_info(world_cup_matches)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        expected_columns = ['id_match', 'home_team', 'away_team', 'home_result', 'away_result',
                          'result', 'date', 'round', 'city', 'edition']
        for col in expected_columns:
            assert col in column_names

        loader.close()

    def test_load_data(self, loader, sample_df):
        """Test du chargement des données."""
        loader.connect()
        loader.create_schema()
        loader.load_data(sample_df)

        # Vérifier que les données ont été chargées
        df_loaded = pd.read_sql_query("SELECT * FROM world_cup_matches", loader.conn)
        assert len(df_loaded) == 2
        assert df_loaded.iloc[0]['home_team'] == 'Brazil'

        loader.close()

    def test_verify_load(self, loader, sample_df):
        """Test de la vérification du chargement."""
        loader.connect()
        loader.create_schema()
        loader.load_data(sample_df)

        # Ne devrait pas lever d'exception
        loader.verify_load()

        loader.close()

    def test_close(self, loader):
        """Test de la fermeture de la connexion."""
        loader.connect()
        assert loader.conn is not None
        loader.close()
        # Note: sqlite3 connection objects don't set to None when closed
        # We just verify it was connected before

    def test_load_data_without_connection(self, loader, sample_df):
        """Test du chargement sans connexion établie."""
        with pytest.raises(AttributeError):
            loader.load_data(sample_df)

    def test_create_schema_without_connection(self, loader):
        """Test de la création du schéma sans connexion."""
        with pytest.raises(AttributeError):
            loader.create_schema()

    def test_connect_invalid_path(self, temp_db_path):
        """Test de connexion avec chemin invalide."""
        # Utiliser un chemin invalide
        invalid_path = "C:\\nonexistent\\directory\\test.db"
        loader = WorldCupLoader(db_path=invalid_path)
        with pytest.raises(sqlite3.OperationalError):
            loader.connect()

    def test_load_data_with_rollback(self, loader, sample_df):
        """Test chargement avec erreur pour déclencher rollback."""
        loader.connect()
        loader.create_schema()

        # Modifier le DataFrame pour causer une erreur (date invalide)
        bad_df = sample_df.copy()
        bad_df['date'] = 'invalid_date'

        with pytest.raises(Exception):
            loader.load_data(bad_df)

        loader.close()

    def test_verify_load_with_error(self, loader):
        """Test vérification avec erreur de requête."""
        loader.connect()
        # Sans créer de schéma, la requête va échouer mais l'exception est catchée
        # La méthode ne lève pas d'exception, elle log seulement
        loader.verify_load()  # Ne devrait pas lever d'exception
        loader.close()