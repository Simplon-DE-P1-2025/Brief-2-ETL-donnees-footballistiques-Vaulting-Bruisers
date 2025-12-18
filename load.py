import sqlite3
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class WorldCupLoader:
    """
    Module de Chargement simplifié.
    Charge uniquement la table principale des matchs du tournoi.
    """
    
    def __init__(self, db_path="data/worldcup.db"):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"✅ Connexion à {self.db_path} établie")
        except Exception as e:
            logger.error(f"❌ Erreur connexion DB: {e}")
            raise
    
    def create_schema(self):
        """Création du schéma simplifié (Une seule table)."""
        logger.info("🏗️  Création du schéma simplifié...")
        sql = """
        DROP TABLE IF EXISTS world_cup_matches;
        DROP TABLE IF EXISTS stadiums;     -- On nettoie les anciennes tables si elles existent
        DROP TABLE IF EXISTS teams;
        DROP TABLE IF EXISTS tv_channels;
        
        CREATE TABLE world_cup_matches (
            id_match INTEGER PRIMARY KEY,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_result INTEGER NOT NULL,
            away_result INTEGER NOT NULL, 
            result TEXT,
            date DATE NOT NULL,
            round TEXT NOT NULL,
            city TEXT NOT NULL,
            edition TEXT NOT NULL
        );
        
        -- Index pour la performance des requêtes courantes
        CREATE INDEX idx_edition ON world_cup_matches(edition);
        CREATE INDEX idx_teams ON world_cup_matches(home_team, away_team);
        """
        try:
            self.conn.executescript(sql)
            self.conn.commit()
            logger.info("✅ Table world_cup_matches créée.")
        except Exception as e:
            logger.error(f"❌ Erreur création schéma: {e}")
            raise
    
    def load_data(self, df):
        """Chargement des données dans la table unique."""
        logger.info(f"📤 Chargement de {len(df)} matchs...")
        try:
            df_load = df.copy()
            # Formatage Date pour SQLite
            df_load['date'] = df_load['date'].dt.strftime('%Y-%m-%d')
            
            # Insertion directe (les colonnes du DF correspondent maintenant exactement à la table)
            df_load.to_sql('world_cup_matches', self.conn, if_exists='append', index=False)
            
            self.conn.commit()
            logger.info("✅ Données chargées avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur chargement: {e}")
            self.conn.rollback()
            raise

    # Note : La méthode load_additional_data a été supprimée car inutile.

    def verify_load(self):
        """Vérification simple."""
        try:
            count = pd.read_sql_query("SELECT COUNT(*) as t FROM world_cup_matches", self.conn)['t'][0]
            logger.info(f"✅ Base de données finalisée : {count} matchs enregistrés.")
            
            # Petit check pour voir si on a bien viré les préliminaires
            rounds = pd.read_sql_query("SELECT DISTINCT round FROM world_cup_matches", self.conn)
            logger.info(f"ℹ️  Phases présentes : {rounds['round'].tolist()}")
            
        except Exception as e:
            logger.error(f"❌ Erreur vérification: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("✅ Connexion fermée")