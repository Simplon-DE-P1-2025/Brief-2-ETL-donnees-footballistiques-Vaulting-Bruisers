import sqlite3
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class WorldCupLoader:
    """
    Module de Chargement (Load) de l'ETL.
    Responsable de la persistance des données transformées dans une base relationnelle SQLite.
    Gère la définition du schéma (DDL), l'insertion des données et les contrôles de qualité post-chargement.
    """
    
    def __init__(self, db_path="data/worldcup.db"):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Établit la connexion à la base de données SQLite."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            # Configuration pour accéder aux colonnes par leur nom (style dictionnaire)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"✅ Connexion à {self.db_path} établie")
        except Exception as e:
            logger.error(f"❌ Erreur connexion DB: {e}")
            raise
    
    def create_schema(self):
        """
        Exécute le DDL (Data Definition Language).
        Réinitialise et crée la structure des tables (Matchs, Stades, Équipes, TV).
        Définit les clés primaires et les index pour la performance.
        """
        logger.info("🏗️  Création du schéma principal...")
        sql = """
        -- Réinitialisation propre (Idempotence)
        DROP TABLE IF EXISTS world_cup_matches;
        
        -- Table de faits principale
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
            edition TEXT NOT NULL,
            source TEXT,
            match_id_2018 INTEGER,
            stadium_id INTEGER,
            stadium_name TEXT,
            home_fifacode TEXT,
            away_fifacode TEXT
        );
        
        -- Indexation pour optimiser les requêtes analytiques courantes
        CREATE INDEX idx_edition ON world_cup_matches(edition);
        CREATE INDEX idx_teams ON world_cup_matches(home_team, away_team);
        
        -- Tables de dimensions (Référentiels)
        DROP TABLE IF EXISTS stadiums;
        CREATE TABLE IF NOT EXISTS stadiums (
            stadium_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT NOT NULL,
            lat REAL, lng REAL, image TEXT
        );
        
        DROP TABLE IF EXISTS teams;
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            fifaCode TEXT, iso2 TEXT, flag TEXT
        );
        
        DROP TABLE IF EXISTS tv_channels;
        CREATE TABLE IF NOT EXISTS tv_channels (
            channel_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country TEXT, languages TEXT
        );
        """
        try:
            self.conn.executescript(sql)
            self.conn.commit() # Validation de la transaction
            logger.info("✅ Schéma créé avec succès")
        except Exception as e:
            logger.error(f"❌ Erreur création schéma: {e}")
            raise
    
    def load_data(self, df):
        """
        Charge le DataFrame principal des matchs dans la base.
        Gère le formatage des dates pour SQLite et l'alignement des colonnes.
        """
        logger.info(f"📤 Chargement de {len(df)} matchs dans la table principale...")
        try:
            df_load = df.copy()
            
            # SQLite ne gère pas le type DATE natif, on force le string ISO 8601
            df_load['date'] = df_load['date'].dt.strftime('%Y-%m-%d')
            
            # Garantie que toutes les colonnes optionnelles existent (même vides) pour éviter une erreur SQL
            opt_cols = ['source', 'match_id_2018', 'stadium_id', 'stadium_name']
            for col in opt_cols:
                if col not in df_load.columns: df_load[col] = None
                
            cols_to_load = ['id_match', 'home_team', 'away_team', 'home_result', 'away_result',
                           'result', 'date', 'round', 'city', 'edition'] + opt_cols
            
            # Intersection stricte : on n'insère que les colonnes présentes à la fois dans le DF et la Table
            final_cols = [c for c in cols_to_load if c in df_load.columns]
            
            # Insertion en mode 'append' (ajout)
            df_load[final_cols].to_sql('world_cup_matches', self.conn, if_exists='append', index=False)
            self.conn.commit()
            logger.info("✅ Données principales chargées")
        except Exception as e:
            logger.error(f"❌ Erreur chargement: {e}")
            self.conn.rollback() # Annulation en cas d'erreur pour garder la base propre
            raise

    def load_additional_data(self, json_data):
        """
        Charge les tables de dimension (Stades, Équipes, TV) depuis le JSON.
        Utilise le mode 'replace' pour ces référentiels.
        """
        logger.info("📤 Chargement des données supplémentaires...")
        try:
            # 1. Chargement Stades
            if 'stadiums' in json_data and json_data['stadiums']:
                pd.DataFrame(json_data['stadiums']).rename(columns={'id':'stadium_id'}).to_sql('stadiums', self.conn, if_exists='replace', index=False)
                logger.info("  - Stades chargés")
                
            # 2. Chargement Équipes
            if 'teams' in json_data and json_data['teams']:
                df_t = pd.DataFrame(json_data['teams']).rename(columns={'id':'team_id'})
                # Nettoyage : suppression colonne liste 'lang' incompatible SQL direct
                if 'lang' in df_t: df_t = df_t.drop('lang', axis=1)
                df_t.to_sql('teams', self.conn, if_exists='replace', index=False)
                logger.info("  - Équipes chargées")
                
            # 3. Chargement Chaînes TV
            if 'tvchannels' in json_data and json_data['tvchannels']:
                df_tv = pd.DataFrame(json_data['tvchannels']).rename(columns={'id':'channel_id'})
                # Aplatissement : liste de langues -> string séparé par des virgules
                if 'lang' in df_tv: df_tv['languages'] = df_tv['lang'].apply(lambda x: ','.join(x) if isinstance(x, list) else str(x))
                if 'lang' in df_tv: df_tv = df_tv.drop('lang', axis=1)
                df_tv.to_sql('tv_channels', self.conn, if_exists='replace', index=False)
                logger.info("  - Chaînes TV chargées")
                
            self.conn.commit()
        except Exception as e:
            logger.error(f"❌ Erreur données supp: {e}")

    def verify_load(self):
        """
        Audit post-chargement (Data Quality Check).
        Vérifie la volumétrie globale, la distribution par édition et l'intégrité temporelle.
        """
        logger.info("🔍 Vérification détaillée du chargement...")
        try:
            # 1. Volumétrie
            res = pd.read_sql_query("SELECT COUNT(*) as t FROM world_cup_matches", self.conn)
            logger.info(f"✅ Total matchs en base: {res['t'][0]}")
            
            # 2. Distribution (Cohérence historique)
            editions = pd.read_sql_query("SELECT edition, COUNT(*) as n FROM world_cup_matches GROUP BY edition ORDER BY edition", self.conn)
            logger.info("📊 Répartition par édition:")
            for _, row in editions.iterrows():
                logger.info(f"  - {row['edition']}: {row['n']} matchs")
            
            # 3. Bornes temporelles (Test d'intégration)
            first = pd.read_sql_query("SELECT * FROM world_cup_matches WHERE id_match = 1", self.conn)
            if not first.empty:
                logger.info(f"🥇 Premier: {first.iloc[0]['home_team']} vs {first.iloc[0]['away_team']} ({first.iloc[0]['date']})")
                
            last = pd.read_sql_query("SELECT * FROM world_cup_matches ORDER BY id_match DESC LIMIT 1", self.conn)
            if not last.empty:
                logger.info(f"🏆 Dernier: {last.iloc[0]['home_team']} vs {last.iloc[0]['away_team']} ({last.iloc[0]['date']})")
                
        except Exception as e:
            logger.error(f"❌ Erreur vérification: {e}")

    def close(self):
        """Fermeture propre de la connexion pour libérer le fichier DB."""
        if self.conn:
            self.conn.close()
            logger.info("✅ Connexion fermée")
