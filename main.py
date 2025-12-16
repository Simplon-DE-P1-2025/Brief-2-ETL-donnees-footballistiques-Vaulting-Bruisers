"""
ETL Pipeline - FIFA World Cup Matches (1930-2014)
Pipeline complet : Extract -> Transform -> Load
Auteurs : Équipe Data Engineering
Date : Décembre 2024
"""
import json
import pandas as pd
import sqlite3
import re
from datetime import datetime
from unidecode import unidecode
import logging
from pathlib import Path

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =====================================================================
# DICTIONNAIRES DE MAPPING (Data Engineer 2)
# =====================================================================

TEAMS_MAPPING = {
    # Allemagne
    "West Germany": "Germany",
    "FR Germany": "Germany",
    "German DR": "East Germany",
    "Germany FR": "Germany",
    
    # URSS / Russie
    "Soviet Union": "Russia",
    "USSR": "Russia",
    
    # Yougoslavie
    "Yugoslavia": "Serbia",
    
    # Tchécoslovaquie
    "Czechoslovakia": "Czech Republic",
    
    # Corée
    "Korea Republic": "South Korea",
    "Korea DPR": "North Korea",
    "South Korea": "South Korea",
    
    # USA
    "United States": "USA",
    "US": "USA",
    
    # Autres variations
    "Ivory Coast": "Cote d'Ivoire",
    "Côte d'Ivoire": "Cote d'Ivoire",
    "Bosnia-Herzegovina": "Bosnia and Herzegovina",
    "Iran": "Iran",
    "Irish Republic": "Republic of Ireland",
    "Northern Ireland": "Northern Ireland",
    
    # Casse (sera géré automatiquement mais on garde pour historique)
    "FRANCE": "France",
    "BRAZIL": "Brazil",
    "ARGENTINA": "Argentina",
}

CITIES_MAPPING = {
    "MONTEVIDEO": "Montevideo",
    "MEXICO CITY": "Mexico City",
    "Mexico (México)": "Mexico City",
    "México": "Mexico City",
    "Sao Paulo": "São Paulo",
    "São Paulo": "São Paulo",
    "Rio de Janeiro": "Rio de Janeiro",
    "Saint-Denis": "Paris",
    "Saint Denis": "Paris",
    "ROME": "Rome",
    "PARIS": "Paris",
    "BERLIN": "Berlin",
    "LONDON": "London",
    "MADRID": "Madrid",
    "BARCELONA": "Barcelona",
    "BUENOS AIRES": "Buenos Aires",
    "Brasilia": "Brasília",
    "Brasília": "Brasília",
    "Belo Horizonte": "Belo Horizonte",
    "Porto Alegre": "Porto Alegre",
    "Curitiba": "Curitiba",
    "Manaus": "Manaus",
    "Fortaleza": "Fortaleza",
    "Recife": "Recife",
    "Salvador": "Salvador",
    "Natal": "Natal",
    "Cuiaba": "Cuiabá",
}

ROUNDS_MAPPING = {
    # Group Stage
    "GROUP_STAGE": "Group Stage",
    "Group A": "Group Stage",
    "Group B": "Group Stage",
    "Group C": "Group Stage",
    "Group D": "Group Stage",
    "Group E": "Group Stage",
    "Group F": "Group Stage",
    "Group G": "Group Stage",
    "Group H": "Group Stage",
    "Group 1": "Group Stage",
    "Group 2": "Group Stage",
    "Group 3": "Group Stage",
    "Group 4": "Group Stage",
    "Poules": "Group Stage",
    "First round": "Group Stage",
    "Preliminary round": "Group Stage",
    
    # Round of 16
    "8e de finale": "Round of 16",
    "Round of 16": "Round of 16",
    "ROUND_OF_16": "Round of 16",
    "Eighth-finals": "Round of 16",
    
    # Quarter-finals
    "1/4 finale": "Quarter-finals",
    "Quarter-finals": "Quarter-finals",
    "QUARTER_FINALS": "Quarter-finals",
    "Quarterfinals": "Quarter-finals",
    
    # Semi-finals
    "1/2 finale": "Semi-finals",
    "Semi-finals": "Semi-finals",
    "SEMI_FINALS": "Semi-finals",
    "Semifinals": "Semi-finals",
    
    # Third Place
    "3rd place": "Third Place",
    "Match pour la 3e place": "Third Place",
    "Third place": "Third Place",
    "Play-off for third place": "Third Place",
    
    # Final
    "Final": "Final",
    "Finale": "Final",
    "FINAL": "Final",
}

TEAMS_MAPPING_2018 = {
    1: "Russia",
    2: "Saudi Arabia",
    3: "Egypt",
    4: "Uruguay",
    5: "Portugal",
    6: "Spain",
    7: "Morocco",
    8: "Iran",
    9: "France",
    10: "Australia",
    11: "Peru",
    12: "Denmark",
    13: "Argentina",
    14: "Iceland",
    15: "Croatia",
    16: "Nigeria",
    17: "Brazil",
    18: "Switzerland",
    19: "Costa Rica",
    20: "Serbia",
    21: "Germany",
    22: "Mexico",
    23: "Sweden",
    24: "South Korea",
    25: "Belgium",
    26: "Panama",
    27: "Tunisia",
    28: "England",
    29: "Poland",
    30: "Senegal",
    31: "Colombia",
    32: "Japan"
}

# Mapping des stades
STADIUMS_MAPPING_2018 = {
    1: "Luzhniki Stadium",
    2: "Otkrytiye Arena",
    3: "Krestovsky Stadium",
    4: "Kaliningrad Stadium",
    5: "Kazan Arena",
    6: "Nizhny Novgorod Stadium",
    7: "Cosmos Arena",
    8: "Volgograd Arena",
    9: "Mordovia Arena",
    10: "Rostov Arena",
    11: "Fisht Olympic Stadium",
    12: "Central Stadium"
}

# =====================================================================
# CLASSE EXTRACT (Data Engineer 1)
# =====================================================================

class WorldCupExtractor:
    """Extraction des données depuis les 3 sources CSV"""
    
    def __init__(self, data_dir="data/raw"):
        self.data_dir = Path(data_dir)
        
    def extract_source1(self, filename="matches_1930-2010.csv"):
        """
        Extract matches_1930-2010.csv
        Format: edition, round, score, team1, team2, url, venue, year
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            df = pd.read_csv(filepath)
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            logger.debug(f"Colonnes: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise
    
    def extract_source2(self, filename="WorldCupMatches2014.csv"):
        """
        Extract WorldCupMatches2014.csv avec correction des problèmes d'encoding
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            
            # Option 1: Lire avec différentes stratégies
            try:
                # Essayer avec utf-8-sig pour les BOM
                df = pd.read_csv(filepath, sep=';', encoding='utf-8-sig')
                logger.info("🔧 Encodage: utf-8-sig (avec BOM)")
            except:
                # Essayer latin-1
                df = pd.read_csv(filepath, sep=';', encoding='latin-1')
                logger.info("🔧 Encodage: latin-1")
            
            # Nettoyer les guillemets parasites
            def clean_cell(cell):
                if isinstance(cell, str):
                    # Supprimer les séquences "rn""> et autres parasites
                    cell = cell.replace('"rn"">', '').replace('"rn">', '')
                    cell = cell.replace('""', '"').strip('"')
                    # Si ça ressemble encore à un nombre, vérifier
                    if cell.isdigit():
                        logger.warning(f"⚠️  Cellule numérique trouvée: {cell}")
                return cell
            
            # Appliquer le nettoyage à toutes les colonnes de texte
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(clean_cell)
            
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            logger.debug(f"Colonnes: {list(df.columns)}")
            
            # Vérifier spécifiquement les équipes
            if 'Away Team Name' in df.columns:
                logger.info(f"🔍 Équipes away après nettoyage: {df['Away Team Name'].unique()[:5]}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise
        """
        Extract WorldCupMatches2014.csv
        Votre fichier semble avoir un problème de délimitation
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            
            # D'abord, regarder le contenu brut
            with open(filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                second_line = f.readline().strip()
            
            logger.debug(f"Première ligne: {first_line[:100]}...")
            logger.debug(f"Deuxième ligne: {second_line[:100]}...")
            
            # Vérifier le séparateur
            if ';' in first_line and ',' not in first_line.replace('","', ''):
                sep = ';'
                logger.info(f"🔧 Séparateur détecté: point-virgule (;)")
            else:
                sep = ','
                logger.info(f"🔧 Séparateur détecté: virgule (,)")
            
            # Essayer de lire avec pandas
            df = pd.read_csv(filepath, sep=sep, encoding='utf-8')
            
            # Si on a toujours un problème (toutes les données dans une colonne)
            if len(df.columns) == 1:
                logger.warning("⚠️  Toutes les données dans une colonne, tentative de correction...")
                
                # Lire le fichier ligne par ligne
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Analyser l'en-tête
                header = lines[0].strip().split(sep)
                logger.debug(f"Header splitté: {header}")
                
                # Nettoyer les guillemets
                header = [col.strip().replace('"', '') for col in header]
                
                # Lire les données
                data = []
                for line in lines[1:]:
                    row = line.strip().split(sep)
                    # Nettoyer les guillemets
                    row = [cell.strip().replace('"', '') for cell in row]
                    data.append(row)
                
                # Créer DataFrame
                df = pd.DataFrame(data, columns=header)
            
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            logger.debug(f"Colonnes: {list(df.columns)}")
            logger.debug(f"Aperçu des premières lignes:\n{df.head(3)}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            logger.error("Veuillez vérifier le format de votre fichier CSV")
            raise
    
    def extract_source3(self, filename="Fifa_world_cup_matches.csv"):
        """
        Extract Fifa_world_cup_matches.csv
        Format supposé: Contient plusieurs éditions
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            
            # Lire le fichier avec différents séparateurs possibles
            try:
                # Essayer d'abord avec la virgule
                df = pd.read_csv(filepath, encoding='utf-8')
                logger.info("🔧 Séparateur détecté: virgule (,)")
            except Exception:
                # Si échec, essayer point-virgule
                df = pd.read_csv(filepath, sep=';', encoding='utf-8')
                logger.info("🔧 Séparateur détecté: point-virgule (;)")
            
            # Nettoyer les noms de colonnes
            df.columns = [col.strip() for col in df.columns]
            
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            logger.info(f"📊 Colonnes: {list(df.columns)}")
            logger.debug(f"Aperçu:\n{df.head(3)}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise

    def extract_source4(self, filename="data_2018.json"):
        """
        Extract data_2018.json - Coupe du Monde 2018
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            
            # Charger le JSON
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"✅ JSON 2018 chargé avec succès")
            logger.info(f"📊 Structure: {list(data.keys())}")
            
            # Afficher les statistiques
            logger.info(f"""
    📊 Statistiques source 4 (2018):
    - Stades: {len(data.get('stadiums', []))}
    - Équipes: {len(data.get('teams', []))}
    - Groupes: {len(data.get('groups', {}))}
    - Matchs de groupe: {sum(len(group['matches']) for group in data.get('groups', {}).values())}
    - Matchs à élimination directe: {len(data.get('knockout', {}).get('round_16', {}).get('matches', [])) +
                                    len(data.get('knockout', {}).get('round_8', {}).get('matches', [])) +
                                    len(data.get('knockout', {}).get('round_4', {}).get('matches', [])) +
                                    len(data.get('knockout', {}).get('round_2_loser', {}).get('matches', [])) +
                                    len(data.get('knockout', {}).get('round_2', {}).get('matches', []))}
            """)
            
            return data
            
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise        
        # =====================================================================
# CLASSE TRANSFORM (Data Engineer 2)
# =====================================================================

class WorldCupTransformer:
    """Transformation et nettoyage des données"""
    
    def __init__(self):
        self.teams_mapping = TEAMS_MAPPING
        self.cities_mapping = CITIES_MAPPING
        self.rounds_mapping = ROUNDS_MAPPING
        self.stadiums_mapping = STADIUMS_MAPPING_2018
        self.teams_mapping_2018 = TEAMS_MAPPING_2018
    
    # -------------------- Fonctions utilitaires --------------------

    @staticmethod
    def parse_score(score_str):
        """
        Parse le score au format '4-1 (3-0)' ou '2-2'
        Returns: (home_goals, away_goals) ou (0, 0) si erreur
        """
        if pd.isna(score_str) or score_str is None:
            return 0, 0
        
        try:
            # Convertir en string et nettoyer
            score_clean = str(score_str).strip()
            
            # Si c'est déjà un tuple/liste (bug possible)
            if isinstance(score_str, (tuple, list)) and len(score_str) == 2:
                return int(score_str[0]), int(score_str[1])
            
            # Si c'est un nombre seul (bug)
            if score_clean.isdigit():
                logger.warning(f"⚠️  Score numérique seul: {score_clean}")
                return int(score_clean), 0
            
            # Regex pour capturer X-Y au début
            match = re.match(r'(\d+)-(\d+)', score_clean)
            if match:
                home = int(match.group(1))
                away = int(match.group(2))
                return max(0, home), max(0, away)  # S'assurer que c'est >= 0
            
            # Essayer d'autres formats
            if ':' in score_clean:
                parts = score_clean.split(':')
                if len(parts) == 2:
                    return int(parts[0]), int(parts[1])
            
            logger.warning(f"⚠️  Score non parsable: {repr(score_str)}")
            return 0, 0
        except Exception as e:
            logger.warning(f"⚠️  Erreur parsing score {repr(score_str)}: {e}")
            return 0, 0

        # AVANT d'appliquer normalize_team
    
    def normalize_team(self, team_name):
        """Normalise le nom d'une équipe - VERSION RENFORCÉE"""
        # DEBUG: Afficher ce qui est reçu
        debug_info = f"normalize_team reçoit: {repr(team_name)} (type: {type(team_name)})"
        
        if pd.isna(team_name):
            logger.warning(f"⚠️  {debug_info} → NaN")
            return "Unknown"
        
        # Convertir en string
        try:
            team = str(team_name)
        except:
            logger.error(f"❌ Impossible de convertir en string: {repr(team_name)}")
            return "Unknown"
        
        team = team.strip()
        
        # DÉTECTION SPÉCIFIQUE DES PROBLÈMES
        if team.isdigit():
            logger.error(f"❌ ÉQUIPE NUMÉRIQUE: {team}")
            logger.error(f"  Valeur brute: {repr(team_name)}")
            logger.error(f"  Type: {type(team_name)}")
            
            # Essayer de récupérer le vrai nom
            if isinstance(team_name, (int, float)):
                # Si c'est un nombre, c'est probablement une erreur d'index
                return "Unknown"
            
            return "Unknown"
        
        # Si c'est très court ou suspect
        if len(team) <= 2 and team not in ['US', 'DR', 'FR', 'UK']:
            logger.warning(f"⚠️  Nom d'équipe suspect (trop court): {team}")
        
        # Nettoyer les guillemets
        team = team.replace('"', '').strip()
        
        # Éviter les chaînes vides
        if team == '':
            return "Unknown"
        
        # Mapping historique
        if team in self.teams_mapping:
            team = self.teams_mapping[team]
        
        # Normalisation casse (Title Case)
        team = team.title()
        
        # Correction d'encodage
        if "C�Te" in team or "Côte" in team or "Cote" in team:
            team = "Cote d'Ivoire"
        
        return team

    def normalize_city(self, city_name):
        """Normalise le nom d'une ville"""
        if pd.isna(city_name):
            return None
        
        city = str(city_name).strip()
        
        # Nettoyage guillemets
        city = city.replace('"', '')
        
        # Nettoyage parenthèses
        city = re.sub(r'\([^)]*\)', '', city).strip()
        
        # Mapping manuel
        if city in self.cities_mapping:
            city = self.cities_mapping[city]
        
        # Title Case si pas dans mapping
        city = city.title()
        
        return city
    
    def normalize_round(self, round_str):
        """Normalise le nom du tour"""
        if pd.isna(round_str):
            return None
        
        round_clean = str(round_str).strip()
        
        # Nettoyage guillemets
        round_clean = round_clean.replace('"', '')
        
        # Mapping exact
        if round_clean in self.rounds_mapping:
            return self.rounds_mapping[round_clean]
        
        # Détection groupe (Group X, Poule X)
        if 'group' in round_clean.lower() or 'poule' in round_clean.lower():
            return "Group Stage"
        
        # Si rien trouvé, Title Case
        return round_clean.title()
    
    @staticmethod
    def compute_result(home_goals, away_goals, home_team=None, away_team=None):
        """Calcule le résultat du match avec nom du gagnant"""
        if pd.isna(home_goals) or pd.isna(away_goals):
            return None
        
        if home_goals > away_goals:
            if home_team:
                return f"{home_team}"  # Nom de l'équipe à domicile
            else:
                return "home_team"
        elif away_goals > home_goals:
            if away_team:
                return f"{away_team}"  # Nom de l'équipe à l'extérieur
            else:
                return "away_team"
        else:
            return "draw"  # Match nul
    
    @staticmethod
    def parse_datetime(datetime_str):
        """
        Parse diverses formats de dates
        Exemples: 
        - '12 Jun 2014 - 17:00'
        - '1930-07-13'
        - '13 Jul 1930'
        """
        if pd.isna(datetime_str):
            return None
        
        datetime_str = str(datetime_str).strip().replace('"', '')
        
        try:
            # Format: '12 Jun 2014 - 17:00'
            if ' - ' in datetime_str:
                date_part = datetime_str.split(' - ')[0].strip()
                return pd.to_datetime(date_part, format='%d %b %Y')
            
            # Format: '13 Jul 1930'
            if len(datetime_str.split()) == 3:
                return pd.to_datetime(datetime_str, format='%d %b %Y')
            
            # Format ISO
            return pd.to_datetime(datetime_str)
        except Exception as e:
            logger.warning(f"⚠️  Date non parsable '{datetime_str}': {e}")
            return None
    
    # -------------------- Transformations par source --------------------
    
    def transform_source1(self, df):
        """
        Transformation matches_1930-2010.csv
        """
        logger.info("🔄 Transformation Source 1 (1930-2010)...")
        logger.debug(f"Colonnes reçues: {list(df.columns)}")
        
                # ========== FILTRE IMPORTANT : ENLEVER 2014 ==========
        # Le fichier contient des doublons avec source 2
        if 'year' in df.columns:
            avant = len(df)
            df = df[df['year'] != 2014]  # Enlève 2014
            apres = len(df)
            logger.info(f"✅ Supprimé {avant - apres} matchs de 2014")

        df_clean = df.copy()
        
        # CRITIQUE: Vérifier et normaliser les noms de colonnes
        # Le fichier original a: edition, round, score, team1, team2, url, venue, year
        column_mapping = {}
        
        # Chercher les colonnes d'équipes
        team1_cols = [col for col in df_clean.columns if 'team1' in col.lower() or 'home' in col.lower()]
        team2_cols = [col for col in df_clean.columns if 'team2' in col.lower() or 'away' in col.lower()]
        
        column_mapping['team1'] = team1_cols[0] if team1_cols else df_clean.columns[3]  # team1 est normalement en position 3
        column_mapping['team2'] = team2_cols[0] if team2_cols else df_clean.columns[4]  # team2 est normalement en position 4
        
        logger.debug(f"Mapping colonnes détecté: {column_mapping}")
        
        # Parser score avec gestion des erreurs
        logger.debug("Parsing des scores...")
        scores = df_clean['score'].apply(self.parse_score)
        df_clean['home_result'] = scores.apply(lambda x: x[0])
        df_clean['away_result'] = scores.apply(lambda x: x[1])
        
        # Vérifier les scores nuls ou négatifs
        null_scores = ((df_clean['home_result'] == 0) & (df_clean['away_result'] == 0)).sum()
        if null_scores > 0:
            logger.warning(f"⚠️  {null_scores} matchs avec score 0-0 ou non parsable")
        
        # Normaliser équipes - CRITIQUE: créer home_team et away_team
        df_clean['home_team'] = df_clean[column_mapping['team1']].apply(self.normalize_team)
        df_clean['away_team'] = df_clean[column_mapping['team2']].apply(self.normalize_team)
        
        # Vérifier que les colonnes sont créées
        logger.debug(f"Colonnes après création: {list(df_clean.columns)}")
        
        # Normaliser ville
        venue_cols = [col for col in df_clean.columns if 'venue' in col.lower() or 'city' in col.lower()]
        venue_col = venue_cols[0] if venue_cols else df_clean.columns[6]  # venue est normalement en position 6
        
        df_clean['city'] = df_clean[venue_col].apply(self.normalize_city)
        
        # Edition (extraire année)
        edition_cols = [col for col in df_clean.columns if 'edition' in col.lower()]
        if edition_cols:
            df_clean['edition'] = df_clean[edition_cols[0]].astype(str).str.split('-').str[0]
        else:
            # Fallback: utiliser l'année
            year_cols = [col for col in df_clean.columns if 'year' in col.lower()]
            if year_cols:
                df_clean['edition'] = df_clean[year_cols[0]].astype(str)
            else:
                logger.warning("⚠️  Colonne 'edition' ou 'year' non trouvée")
                df_clean['edition'] = 'Unknown'
        
        # Round
        round_cols = [col for col in df_clean.columns if 'round' in col.lower()]
        round_col = round_cols[0] if round_cols else df_clean.columns[1]  # round est normalement en position 1
        
        df_clean['round'] = df_clean[round_col].apply(self.normalize_round)
        
        # Result
        df_clean['result'] = df_clean.apply(
            lambda row: self.compute_result(
                row['home_result'], 
                row['away_result'],
                row['home_team'],
                row['away_team']
            ), 
            axis=1
)
        
        # Date: ⚠️ Problème - seulement année disponible
        # Solution temporaire: utiliser 1er juillet de chaque année
        year_cols = [col for col in df_clean.columns if 'year' in col.lower()]
        if year_cols:
            df_clean['date'] = df_clean[year_cols[0]].apply(
                lambda y: f"{y}-07-01" if pd.notna(y) else None
            )
            df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
        else:
            logger.error("❌ Colonne 'year' non trouvée pour la date")
            df_clean['date'] = None
        
        logger.warning("⚠️  Dates approximatives (1er juillet) - À enrichir ultérieurement")
        
        # VÉRIFICATION CRITIQUE: s'assurer que toutes les colonnes nécessaires existent
        required_cols = ['home_team', 'away_team', 'home_result', 'away_result',
                        'result', 'date', 'round', 'city', 'edition']
        
        missing_cols = []
        for col in required_cols:
            if col not in df_clean.columns:
                missing_cols.append(col)
                logger.error(f"❌ Colonne manquante: {col}")
        
        if missing_cols:
            raise ValueError(f"Colonnes manquantes après transformation: {missing_cols}")
        
        # Sélection colonnes finales
        result_df = df_clean[required_cols].copy()
        
        # Afficher un aperçu pour débogage
        logger.info(f"✅ Source 1 transformée: {len(result_df)} matchs")
        logger.debug(f"Aperçu des premières lignes:\n{result_df.head(3)}")
        logger.debug(f"Types de données:\n{result_df.dtypes}")
        
        return result_df
    
    def transform_source2(self, df):
        """
        Transformation WorldCupMatches2014.csv - VERSION CORRIGÉE
        """
        logger.info("🔄 Transformation Source 2 (2014)...")
        logger.info(f"📊 Shape: {df.shape}")
        logger.info(f"🔧 Colonnes: {list(df.columns)}")
        
        # AFFICHER UN EXEMPLE COMPLET POUR DÉBOGAGE
        logger.info("🔍 Premier match brut:")
        first_row = df.iloc[0]
        for col in ['Year', 'Home Team Name', 'Home Team Goals', 'Away Team Goals', 'Away Team Name', 'Datetime']:
            if col in first_row:
                logger.info(f"  {col}: {repr(first_row[col])}")
        
        df_clean = df.copy()
        
        # 1. CORRECTION CRITIQUE : Convertir les scores en numérique AVANT
        logger.info("🔧 Conversion des scores...")
        
        # Home goals
        if 'Home Team Goals' in df_clean.columns:
            logger.info(f"  Home Goals avant conversion: {df_clean['Home Team Goals'].head(3).tolist()}")
            df_clean['home_result'] = pd.to_numeric(
                df_clean['Home Team Goals'], 
                errors='coerce'
            ).fillna(0).astype(int)
            logger.info(f"  Home Goals après conversion: {df_clean['home_result'].head(3).tolist()}")
        else:
            logger.error("❌ Colonne 'Home Team Goals' non trouvée")
            logger.error(f"Colonnes disponibles: {list(df_clean.columns)}")
            raise ValueError("Colonne Home Team Goals manquante")
        
        # Away goals
        if 'Away Team Goals' in df_clean.columns:
            logger.info(f"  Away Goals avant conversion: {df_clean['Away Team Goals'].head(3).tolist()}")
            df_clean['away_result'] = pd.to_numeric(
                df_clean['Away Team Goals'], 
                errors='coerce'
            ).fillna(0).astype(int)
            logger.info(f"  Away Goals après conversion: {df_clean['away_result'].head(3).tolist()}")
        else:
            logger.error("❌ Colonne 'Away Team Goals' non trouvée")
            raise ValueError("Colonne Away Team Goals manquante")
        
        # 2. Normaliser les noms d'équipes
        logger.info("🔧 Normalisation des équipes...")
        
        if 'Home Team Name' in df_clean.columns:
            logger.info(f"  Home teams avant: {df_clean['Home Team Name'].head(3).tolist()}")
            df_clean['home_team'] = df_clean['Home Team Name'].apply(self.normalize_team)
            logger.info(f"  Home teams après: {df_clean['home_team'].head(3).tolist()}")
        else:
            logger.error("❌ Colonne 'Home Team Name' non trouvée")
            raise ValueError("Colonne Home Team Name manquante")
        
        if 'Away Team Name' in df_clean.columns:
            logger.info(f"  Away teams avant: {df_clean['Away Team Name'].head(3).tolist()}")
            df_clean['away_team'] = df_clean['Away Team Name'].apply(self.normalize_team)
            logger.info(f"  Away teams après: {df_clean['away_team'].head(3).tolist()}")
        else:
            logger.error("❌ Colonne 'Away Team Name' non trouvée")
            raise ValueError("Colonne Away Team Name manquante")
        
        # 3. VÉRIFICATION : Afficher les scores complets
        logger.info("🔍 Vérification scores complets (5 premiers):")
        for i in range(min(5, len(df_clean))):
            row = df_clean.iloc[i]
            logger.info(f"  {i+1}: {row['home_team']} {row['home_result']}-{row['away_result']} {row['away_team']}")
        
        # 4. Calculer le résultat AVEC NOMS D'ÉQUIPES
        logger.info("🔧 Calcul du résultat...")
        
        def safe_compute_result(row):
            """Version sécurisée de compute_result"""
            try:
                return self.compute_result(
                    row['home_result'], 
                    row['away_result'],
                    row['home_team'],
                    row['away_team']
                )
            except Exception as e:
                logger.warning(f"Erreur compute_result: {e}")
                return "draw"
        
        df_clean['result'] = df_clean.apply(safe_compute_result, axis=1)
        
        # Vérifier les résultats
        logger.info("🔍 Vérification résultats (5 premiers):")
        for i in range(min(5, len(df_clean))):
            row = df_clean.iloc[i]
            logger.info(f"  {i+1}: {row['home_team']} vs {row['away_team']} → {row['result']}")
        
        # 5. Autres colonnes
        logger.info("🔧 Traitement des autres colonnes...")
        
        # City
        if 'City' in df_clean.columns:
            df_clean['city'] = df_clean['City'].apply(self.normalize_city)
            logger.info(f"  Villes: {df_clean['city'].unique()[:5]}")
        else:
            logger.warning("⚠️  Colonne 'City' non trouvée")
            df_clean['city'] = 'Unknown'
        
        # Round
        if 'Stage' in df_clean.columns:
            df_clean['round'] = df_clean['Stage'].apply(self.normalize_round)
            logger.info(f"  Rounds: {df_clean['round'].unique()}")
        else:
            logger.warning("⚠️  Colonne 'Stage' non trouvée")
            df_clean['round'] = 'Group Stage'
        
        # Edition
        if 'Year' in df_clean.columns:
            df_clean['edition'] = df_clean['Year'].astype(str)
            logger.info(f"  Édition: {df_clean['edition'].iloc[0]}")
        else:
            logger.warning("⚠️  Colonne 'Year' non trouvée")
            df_clean['edition'] = '2014'
        
        # Date
        if 'Datetime' in df_clean.columns:
            df_clean['date'] = df_clean['Datetime'].apply(self.parse_datetime)
            logger.info(f"  Dates: {df_clean['date'].iloc[0]} - {df_clean['date'].iloc[-1]}")
        else:
            logger.warning("⚠️  Colonne 'Datetime' non trouvée")
            df_clean['date'] = pd.to_datetime('2014-07-01')
        
        # 6. Vérifier les colonnes finales
        required_cols = ['home_team', 'away_team', 'home_result', 'away_result',
                        'result', 'date', 'round', 'city', 'edition']
        
        missing_cols = [col for col in required_cols if col not in df_clean.columns]
        if missing_cols:
            logger.error(f"❌ Colonnes manquantes: {missing_cols}")
            logger.error(f"Colonnes disponibles: {list(df_clean.columns)}")
            raise ValueError(f"Colonnes manquantes: {missing_cols}")
        
        result_df = df_clean[required_cols].copy()
        
        # 7. VÉRIFICATION FINALE DÉTAILLÉE
        logger.info("🔍 VÉRIFICATION FINALE DÉTAILLÉE:")
        logger.info(f"📊 Nombre de matchs: {len(result_df)}")
        
        # Analyser les résultats
        results_dist = result_df['result'].value_counts()
        logger.info("📈 Distribution des résultats:")
        for result, count in results_dist.head(10).items():
            logger.info(f"  {result}: {count} matchs ({count/len(result_df)*100:.1f}%)")
        
        # Vérifier les matchs nuls
        draws = result_df[result_df['result'] == 'draw']
        logger.info(f"🔍 {len(draws)} matchs nuls:")
        for i in range(min(3, len(draws))):
            row = draws.iloc[i]
            logger.info(f"  {row['home_team']} {row['home_result']}-{row['away_result']} {row['away_team']}")
        
        # Vérifier les problèmes potentiels
        suspicious = result_df[result_df['result'].astype(str).str.isdigit()]
        if len(suspicious) > 0:
            logger.warning(f"⚠️  {len(suspicious)} résultats suspects (numériques):")
            for i in range(min(3, len(suspicious))):
                row = suspicious.iloc[i]
                logger.warning(f"  Problème: {row['home_team']} vs {row['away_team']} → {row['result']}")
        
        logger.info(f"✅ Source 2 transformée: {len(result_df)} matchs")
        return result_df
        """
        Transformation WorldCupMatches2014.csv
        """
        logger.info("🔄 Transformation Source 2 (2014)...")
        logger.debug(f"Colonnes reçues: {list(df.columns)}")
        logger.debug(f"Shape: {df.shape}")
        
        if df.shape[1] <= 1:
            logger.error("❌ Le DataFrame n'a qu'une seule colonne")
            logger.error(f"Contenu de la colonne unique: {df.iloc[:, 0].head(5).tolist()}")
            raise ValueError("Fichier CSV mal formaté - toutes les données dans une colonne")
        
        df_clean = df.copy()
        
        # Normaliser les noms de colonnes
        df_clean.columns = [col.strip().replace('"', '').replace(';', '') for col in df_clean.columns]
        logger.info(f"🔧 Colonnes après nettoyage: {list(df_clean.columns)}")
        
        # Afficher les premières lignes pour débogage
        logger.debug(f"Aperçu des données:\n{df_clean.head(3)}")
        
        # Chercher les colonnes nécessaires avec des patterns flexibles
        column_mapping = {}
        
        # Fonction pour trouver une colonne par patterns
        def find_column(patterns, default_position=None):
            for pattern in patterns:
                for col in df_clean.columns:
                    if pattern in col.lower():
                        return col
            # Si pas trouvé et position par défaut spécifiée
            if default_position is not None and len(df_clean.columns) > default_position:
                return df_clean.columns[default_position]
            return None
        
        # Mapping des colonnes
        column_mapping['home_team'] = find_column(['home team', 'hometeam', 'team1'], 5)
        column_mapping['away_team'] = find_column(['away team', 'awayteam', 'team2'], 8)
        column_mapping['home_goals'] = find_column(['home goal', 'homegoals', 'home team goal'], 6)
        column_mapping['away_goals'] = find_column(['away goal', 'awaygoals', 'away team goal'], 7)
        column_mapping['city'] = find_column(['city', 'venue', 'stadium city'], 4)
        column_mapping['stage'] = find_column(['stage', 'round', 'phase'], 2)
        column_mapping['year'] = find_column(['year', 'edition', 'tournament'], 0)
        column_mapping['datetime'] = find_column(['datetime', 'date', 'match date', 'time'], 1)
        
        logger.info(f"🔍 Mapping détecté:")
        for key, value in column_mapping.items():
            logger.info(f"  {key}: {value}")
        
        # CRITIQUE: Vérifier que les colonnes critiques existent
        required_sources = ['home_team', 'away_team', 'home_goals', 'away_goals']
        missing_sources = [key for key in required_sources if column_mapping[key] is None]
        
        if missing_sources:
            logger.error(f"❌ Colonnes sources manquantes: {missing_sources}")
            logger.error(f"Colonnes disponibles: {list(df_clean.columns)}")
            raise ValueError(f"Colonnes sources manquantes: {missing_sources}")
        
        # 1. Créer les colonnes standardisées d'équipes
        df_clean['home_team'] = df_clean[column_mapping['home_team']].apply(self.normalize_team)
        df_clean['away_team'] = df_clean[column_mapping['away_team']].apply(self.normalize_team)
        
        # 2. Créer les colonnes de scores
        df_clean['home_result'] = pd.to_numeric(
            df_clean[column_mapping['home_goals']], 
            errors='coerce'
        ).fillna(0).astype(int).clip(lower=0)
        
        df_clean['away_result'] = pd.to_numeric(
            df_clean[column_mapping['away_goals']], 
            errors='coerce'
        ).fillna(0).astype(int).clip(lower=0)
        
        logger.info(f"📊 Scores transformés: {len(df_clean)} lignes")
        logger.debug(f"Exemple de scores: {df_clean[['home_team', 'home_result', 'away_result', 'away_team']].head(3).to_string()}")
        
        # 3. Créer la colonne result
        df_clean['result'] = df_clean.apply(
            lambda row: self.compute_result(
                row['home_result'], 
                row['away_result'],
                row['home_team'],
                row['away_team']
            ), 
            axis=1
        )
                
        # 4. Créer la colonne city
        if column_mapping['city']:
            df_clean['city'] = df_clean[column_mapping['city']].apply(self.normalize_city)
        else:
            logger.warning("⚠️  Colonne 'city' non trouvée, utilisation de 'Unknown'")
            df_clean['city'] = 'Unknown'
        
        # 5. Créer la colonne round
        if column_mapping['stage']:
            df_clean['round'] = df_clean[column_mapping['stage']].apply(self.normalize_round)
        else:
            logger.warning("⚠️  Colonne 'stage' non trouvée, utilisation de 'Group Stage' par défaut")
            df_clean['round'] = 'Group Stage'
        
        # 6. Créer la colonne edition (année)
        if column_mapping['year']:
            df_clean['edition'] = pd.to_numeric(
                df_clean[column_mapping['year']], 
                errors='coerce'
            ).fillna(0).astype(int).astype(str)
            # Remplacer '0' par 'Unknown'
            df_clean['edition'] = df_clean['edition'].replace('0', 'Unknown')
        else:
            logger.warning("⚠️  Colonne 'year' non trouvée, utilisation de 'Unknown'")
            df_clean['edition'] = 'Unknown'
        
        # 7. Créer la colonne date
        if column_mapping['datetime']:
            df_clean['date'] = df_clean[column_mapping['datetime']].apply(self.parse_datetime)
            # Si certaines dates sont NULL, essayer de les compléter
            null_dates = df_clean['date'].isnull().sum()
            if null_dates > 0:
                logger.warning(f"⚠️  {null_dates} dates non parsables, tentative de complétion...")
                # Utiliser l'année + 1er juillet comme fallback
                for idx, row in df_clean[df_clean['date'].isnull()].iterrows():
                    if row['edition'] != 'Unknown' and row['edition'].isdigit():
                        df_clean.at[idx, 'date'] = pd.to_datetime(f"{row['edition']}-07-01")
        else:
            logger.warning("⚠️  Colonne 'datetime' non trouvée, création à partir de l'édition")
            df_clean['date'] = df_clean['edition'].apply(
                lambda x: pd.to_datetime(f"{x}-07-01") if x != 'Unknown' and x.isdigit() else pd.NaT
            )
        
        # Vérifier les dates invalides
        invalid_dates = df_clean['date'].isnull().sum()
        if invalid_dates > 0:
            logger.warning(f"⚠️  {invalid_dates} dates invalides après traitement")
            # Remplacer par une date par défaut pour éviter les erreurs
            default_date = pd.to_datetime('1900-01-01')
            df_clean['date'] = df_clean['date'].fillna(default_date)
        
        # VÉRIFICATION FINALE: s'assurer que toutes les colonnes existent
        required_final_cols = ['home_team', 'away_team', 'home_result', 'away_result',
                            'result', 'date', 'round', 'city', 'edition']
        
        logger.info("🔍 Vérification des colonnes finales...")
        for col in required_final_cols:
            if col not in df_clean.columns:
                logger.error(f"❌ Colonne finale manquante: {col}")
            else:
                logger.debug(f"✅ Colonne présente: {col} (type: {df_clean[col].dtype})")
        
        # Sélectionner uniquement les colonnes nécessaires
        result_df = df_clean[required_final_cols].copy()
        
        # Statistiques de transformation
        logger.info(f"""
    📊 RÉSUMÉ TRANSFORMATION SOURCE 2:
    - Matchs transformés: {len(result_df)}
    - Scores valides: {(result_df['home_result'] > 0) | (result_df['away_result'] > 0).sum()}
    - Éditions uniques: {result_df['edition'].nunique()}
    - Villes uniques: {result_df['city'].nunique()}
    - Premier match: {result_df.iloc[0]['home_team']} vs {result_df.iloc[0]['away_team']}
    - Dernier match: {result_df.iloc[-1]['home_team']} vs {result_df.iloc[-1]['away_team']}
        """)
        
        logger.debug(f"Aperçu final:\n{result_df.head(3).to_string()}")
        
        return result_df
        
    def transform_source3(self, df):
        """
        Transformation Fifa_world_cup_matches.csv
        """
        logger.info("🔄 Transformation Source 3 (Fifa_world_cup_matches)...")
        
        # AFFICHER pour vérifier
        logger.info("🔍 Vérification des colonnes de score:")
        for col in df.columns:
            if 'goal' in col.lower() or 'score' in col.lower():
                logger.info(f"  '{col}' → {df[col].head(3).tolist()}")
        
        df_clean = df.copy()
        
        # Chercher les colonnes CORRECTES
        col_mapping = {}
        
       # 1. Équipes - LES COLONNES EXACTES
        col_mapping['home_team'] = 'team1'
        col_mapping['away_team'] = 'team2'

        # Vérifier qu'elles existent
        if 'team1' not in df_clean.columns:
            logger.error("❌ Colonne 'team1' non trouvée")
            return pd.DataFrame()
        if 'team2' not in df_clean.columns:
            logger.error("❌ Colonne 'team2' non trouvée")
            return pd.DataFrame()

        logger.info("✅ Colonnes équipes: 'team1' et 'team2'")
        # 2. SCORES - IMPORTANT : Utiliser 'number of goals team1/team2'
        for col in df_clean.columns:
            if 'number of goals team1' in col.lower():
                col_mapping['home_goals'] = col
                logger.info(f"✅ Score domicile: {col}")
            elif 'number of goals team2' in col.lower():
                col_mapping['away_goals'] = col
                logger.info(f"✅ Score extérieur: {col}")
        
        logger.info(f"🔍 Mapping final: {col_mapping}")
        
        # Si pas trouvé, chercher autrement
        if 'home_goals' not in col_mapping:
            logger.warning("⚠️  Colonne 'number of goals team1' non trouvée, recherche alternative...")
            for col in df_clean.columns:
                if 'goal' in col.lower() and 'team1' in col.lower():
                    col_mapping['home_goals'] = col
                    logger.info(f"🔧 Score domicile alternatif: {col}")
        
        if 'away_goals' not in col_mapping:
            for col in df_clean.columns:
                if 'goal' in col.lower() and 'team2' in col.lower():
                    col_mapping['away_goals'] = col
                    logger.info(f"🔧 Score extérieur alternatif: {col}")
        
        # VÉRIFICATION CRITIQUE
        required = ['home_team', 'away_team', 'home_goals', 'away_goals']
        missing = [col for col in required if col not in col_mapping]
        
        if missing:
            logger.error(f"❌ Colonnes manquantes: {missing}")
            logger.error(f"Colonnes disponibles: {list(df_clean.columns)}")
            return pd.DataFrame()
        
        # Créer le DataFrame avec les BONNES colonnes
        result_df = pd.DataFrame()
        
        # Équipes
        result_df['home_team'] = df_clean[col_mapping['home_team']].apply(self.normalize_team)
        result_df['away_team'] = df_clean[col_mapping['away_team']].apply(self.normalize_team)
        
        # SCORES (en utilisant les bonnes colonnes)
        result_df['home_result'] = pd.to_numeric(
            df_clean[col_mapping['home_goals']], 
            errors='coerce'
        ).fillna(0).astype(int)
        
        result_df['away_result'] = pd.to_numeric(
            df_clean[col_mapping['away_goals']], 
            errors='coerce'
        ).fillna(0).astype(int)
        
        # Vérifier les scores
        logger.info("🔍 Vérification scores (5 premiers):")
        for i in range(min(5, len(result_df))):
            logger.info(f"  {result_df.iloc[i]['home_team']} {result_df.iloc[i]['home_result']}-{result_df.iloc[i]['away_result']} {result_df.iloc[i]['away_team']}")
        
        # 3. Résultat AVEC NOM DU GAGNANT
        result_df['result'] = result_df.apply(
            lambda row: self.compute_result(
                row['home_result'], 
                row['away_result'],
                row['home_team'],
                row['away_team']
            ),
            axis=1
        )
        # 4. Date
        for col in df_clean.columns:
            if 'date' in col.lower():
                col_mapping['date'] = col
                logger.info(f"✅ Colonne date: {col}")
                break
        if 'date' in col_mapping:
                # DEBUG : Voir les formats
            sample_dates = df_clean[col_mapping['date']].head(5).tolist()
            logger.info(f"🔍 Format des dates (5 premières): {sample_dates}")
            # Fonction spéciale pour les dates '20Nov22'
            def parse_date_special(date_str):
                if pd.isna(date_str):
                    return None
                
                date_str = str(date_str).strip()
                
                # Format '20Nov22'
                if len(date_str) == 7 and date_str[2:5].isalpha():
                    try:
                        day = date_str[:2]
                        month = date_str[2:5]
                        year = "20" + date_str[5:]  # "22" -> "2022"
                        formatted = f"{day} {month} {year}"
                        return pd.to_datetime(formatted, format='%d %b %Y')
                    except:
                        return None
                
                # Sinon utiliser parse_datetime normal
                return self.parse_datetime(date_str)
            
            result_df['date'] = df_clean[col_mapping['date']].apply(parse_date_special)
            logger.info(f"🔍 Dates parsées: {result_df['date'].head(3).tolist()}")
            
        else:
            logger.warning("⚠️  Colonne date non trouvée")
            result_df['date'] = None
        
        # 5. Édition/Année
        if 'year' in col_mapping:
            result_df['edition'] = df_clean[col_mapping['year']].astype(str)
        else:
            # Déduire depuis les dates
            if 'date' in result_df.columns:
                # Prendre l'année de la première date
                first_year = result_df['date'].iloc[0].year
                result_df['edition'] = str(first_year)
                logger.info(f"✅ Édition déduite depuis dates: {first_year}")
            else:
                logger.warning("⚠️  Colonne edition/year non trouvée, utilisation '2022' (déduit)")
                result_df['edition'] = '2022'  # Parce que les dates sont en 2022
                
                # 6. Ville
                if 'city' in col_mapping:
                    result_df['city'] = df_clean[col_mapping['city']].apply(self.normalize_city)
                else:
                    result_df['city'] = 'Unknown'
                
                # 7. Round
                if 'round' in col_mapping:
                    result_df['round'] = df_clean[col_mapping['round']].apply(self.normalize_round)
                else:
                    result_df['round'] = 'Group Stage'
                
                # Vérification finale
                logger.info(f"📊 Source 3 transformée: {len(result_df)} matchs")
                
                # Gérer les dates manquantes
                if result_df['date'].isnull().any():
                    logger.warning(f"⚠️  {result_df['date'].isnull().sum()} dates manquantes après traitement")
                    # Remplacer par date par défaut
                    default_date = pd.to_datetime('1900-01-01')
                    result_df['date'] = result_df['date'].fillna(default_date)
                
                logger.debug(f"Aperçu source 3:\n{result_df.head(3)}")
                
                return result_df
            
    def transform_source4(self, json_data):
        """
        Transformation data_2018.json
        """
        logger.info("🔄 Transformation Source 4 (Coupe du Monde 2018)...")
        
        if not json_data:
            logger.warning("⚠️  Données JSON vides")
            return pd.DataFrame()
        
        # Extraire les matchs des groupes
        group_matches = []
        for group_name, group_data in json_data.get('groups', {}).items():
            for match in group_data.get('matches', []):
                match_data = match.copy()
                match_data['type'] = 'group'
                match_data['group'] = group_name
                group_matches.append(match_data)
        
        # Extraire les matchs à élimination directe
        knockout_matches = []
        knockout_rounds = json_data.get('knockout', {})
        
        # Round of 16
        for match in knockout_rounds.get('round_16', {}).get('matches', []):
            match_data = match.copy()
            match_data['type'] = 'knockout'
            match_data['round'] = 'Round of 16'
            knockout_matches.append(match_data)
        
        # Quarter-finals
        for match in knockout_rounds.get('round_8', {}).get('matches', []):
            match_data = match.copy()
            match_data['type'] = 'knockout'
            match_data['round'] = 'Quarter-finals'
            knockout_matches.append(match_data)
        
        # Semi-finals
        for match in knockout_rounds.get('round_4', {}).get('matches', []):
            match_data = match.copy()
            match_data['type'] = 'knockout'
            match_data['round'] = 'Semi-finals'
            knockout_matches.append(match_data)
        
        # Third Place
        for match in knockout_rounds.get('round_2_loser', {}).get('matches', []):
            match_data = match.copy()
            match_data['type'] = 'knockout'
            match_data['round'] = 'Third Place'
            knockout_matches.append(match_data)
        
        # Final
        for match in knockout_rounds.get('round_2', {}).get('matches', []):
            match_data = match.copy()
            match_data['type'] = 'knockout'
            match_data['round'] = 'Final'
            knockout_matches.append(match_data)
        
        # Combiner tous les matchs
        all_matches = group_matches + knockout_matches
        
        logger.info(f"📊 Total matchs extraits: {len(all_matches)}")
        logger.info(f"  - Matchs de groupe: {len(group_matches)}")
        logger.info(f"  - Matchs à élimination: {len(knockout_matches)}")
        
        if len(all_matches) == 0:
            logger.warning("⚠️  Aucun match trouvé dans les données")
            return pd.DataFrame()
        
        # Créer le DataFrame
        matches_list = []
        
        for match in all_matches:
            # Convertir les IDs d'équipes en noms
            home_team_id = match.get('home_team')
            away_team_id = match.get('away_team')
            
            if home_team_id in self.teams_mapping_2018:
                home_team = self.teams_mapping_2018[home_team_id]
            else:
                logger.warning(f"⚠️  ID équipe domicile inconnu: {home_team_id}")
                home_team = f"Unknown_{home_team_id}"
            
            if away_team_id in self.teams_mapping_2018:
                away_team = self.teams_mapping_2018[away_team_id]
            else:
                logger.warning(f"⚠️  ID équipe extérieur inconnu: {away_team_id}")
                away_team = f"Unknown_{away_team_id}"
            
            # Normaliser les noms d'équipes avec le mapping historique
            home_team_norm = self.normalize_team(home_team)
            away_team_norm = self.normalize_team(away_team)
            
            # Extraire le score
            home_result = match.get('home_result', 0)
            away_result = match.get('away_result', 0)
            
           # Calculer le résultat AVEC NOM DU GAGNANT
            result = self.compute_result(home_result, away_result, home_team_norm, away_team_norm)
            
            # Gérer la date
            date_str = match.get('date')
            if date_str:
                # Extraire la partie date seulement (avant 'T')
                date_part = date_str.split('T')[0]
                try:
                    date_obj = pd.to_datetime(date_part)
                except:
                    date_obj = pd.to_datetime('2018-07-01')  # Date par défaut
            else:
                date_obj = pd.to_datetime('2018-07-01')
            
            # Gérer la ville/stade
            stadium_id = match.get('stadium')
            city = "Unknown"
            
            # Chercher la ville dans les données des stades
            if 'stadiums' in json_data:
                for stadium in json_data['stadiums']:
                    if stadium.get('id') == stadium_id:
                        city = stadium.get('city', 'Unknown')
                        break
            
            # Normaliser la ville
            city_norm = self.normalize_city(city)
            
            # Gérer le round
            round_str = match.get('round')
            if not round_str:
                round_str = 'Group Stage' if match.get('type') == 'group' else 'Knockout'
            
            round_norm = self.normalize_round(round_str)
            
            # Ajouter au DataFrame
            match_dict = {
                'home_team': home_team_norm,
                'away_team': away_team_norm,
                'home_result': home_result,
                'away_result': away_result,
                'result': result,
                'date': date_obj,
                'round': round_norm,
                'city': city_norm,
                'edition': '2018',
                'source': 'json_2018',
                'match_id_2018': match.get('name'),
                'stadium_id': stadium_id
            }
            
            matches_list.append(match_dict)
        
        # Créer DataFrame
        df_clean = pd.DataFrame(matches_list)
        
        # Statistiques
        logger.info(f"""
    📊 RÉSUMÉ TRANSFORMATION SOURCE 4 (2018):
    - Matchs transformés: {len(df_clean)}
    - Équipes uniques: {pd.concat([df_clean['home_team'], df_clean['away_team']]).nunique()}
    - Villes uniques: {df_clean['city'].nunique()}
    - Rounds uniques: {df_clean['round'].unique()}
    - Période: {df_clean['date'].min().date()} au {df_clean['date'].max().date()}
        """)
        
        # Afficher quelques exemples
        logger.debug(f"Aperçu des matchs 2018:\n{df_clean.head(3).to_string()}")
        
        # Vérification de base
        if len(df_clean) != 64:
            logger.warning(f"⚠️  Attendu 64 matchs pour 2018, obtenu {len(df_clean)}")
        
        return df_clean
    
    def enrich_with_stadiums(self, df_2018, json_data):
        """
        Enrichir les données avec les informations des stades
        """
        logger.info("🏟️  Enrichissement avec les données des stades...")
        
        if 'stadiums' not in json_data or len(json_data['stadiums']) == 0:
            logger.warning("⚠️  Aucune donnée de stade disponible")
            return df_2018
        
        # Créer un DataFrame des stades
        stadiums_df = pd.DataFrame(json_data['stadiums'])
        
        # Vérifier les colonnes disponibles
        logger.info(f"Colonnes stades disponibles: {list(stadiums_df.columns)}")
        
        # Renommer pour correspondre
        stadiums_df = stadiums_df.rename(columns={
            'id': 'stadium_id',
            'name': 'stadium_name'
        })
        
        # Fusionner avec les matchs (si stadium_id existe)
        if 'stadium_id' in df_2018.columns:
            df_enriched = pd.merge(
                df_2018,
                stadiums_df[['stadium_id', 'stadium_name', 'lat', 'lng']],
                on='stadium_id',
                how='left'
            )
            
            logger.info(f"✅ {df_enriched['stadium_name'].notnull().sum()} matchs enrichis avec données stade")
            return df_enriched
        else:
            logger.warning("⚠️  Colonne stadium_id non trouvée dans les données 2018")
            return df_2018
    
    def enrich_with_teams_info(self, df_2018, json_data):
        """
        Enrichir avec les informations des équipes (drapeaux, codes FIFA)
        """
        logger.info("🏴󠁧󠁢󠁥󠁮󠁧󠁿 Enrichissement avec les données des équipes...")
        
        if 'teams' not in json_data or len(json_data['teams']) == 0:
            logger.warning("⚠️  Aucune donnée d'équipe disponible")
            return df_2018
        
        # Créer un DataFrame des équipes
        teams_df = pd.DataFrame(json_data['teams'])
        
        # Ajouter les informations des équipes pour home_team
        df_enriched = df_2018.copy()
        
        # Pour chaque équipe, essayer de trouver des informations
        for team_type in ['home', 'away']:
            team_col = f'{team_type}_team'
            
            # Créer des colonnes pour les informations d'équipe
            for info_col in ['fifaCode', 'iso2', 'flag']:
                new_col = f'{team_type}_{info_col.lower()}'
                df_enriched[new_col] = None
        
        logger.info(f"✅ Informations d'équipe disponibles pour {len(teams_df)} équipes")
        
        # Note: l'enrichissement complet nécessiterait un mapping nom d'équipe -> ID
        # Pour l'instant, on se contente de marquer que l'information est disponible
        
        return df_enriched
    def consolidate(self, dfs_list):
        """
        Consolide toutes les sources et génère id_match séquentiel
        """
        logger.info("🔗 Consolidation des sources...")
        
        # Filtrer les None
        dfs_list = [df for df in dfs_list if df is not None]
        
        if not dfs_list:
            raise ValueError("Aucune donnée à consolider")
        
        logger.info(f"📊 {len(dfs_list)} sources à consolider")
        for i, df in enumerate(dfs_list, 1):
            logger.info(f"  Source {i}: {len(df)} matchs")
        
        # Concaténation
        df_all = pd.concat(dfs_list, ignore_index=True)
        
        logger.info(f"📊 Total avant nettoyage: {len(df_all)} matchs")
        
        # Supprimer les lignes avec valeurs manquantes critiques
        initial_count = len(df_all)
        df_all = df_all.dropna(subset=['home_team', 'away_team', 'date'])
        logger.info(f"📊 {initial_count - len(df_all)} lignes supprimées (valeurs manquantes)")
        
        # Supprimer doublons (même équipes + même date)
        initial_count = len(df_all)
        df_all = df_all.drop_duplicates(
            subset=['home_team', 'away_team', 'date'],
            keep='first'
        )
        logger.info(f"📊 {initial_count - len(df_all)} doublons supprimés")
        
        logger.info(f"📊 Total après nettoyage: {len(df_all)} matchs")
        
        # Trier chronologiquement
        df_all['date'] = pd.to_datetime(df_all['date'])
        df_all = df_all.sort_values('date').reset_index(drop=True)
        
        # Générer id_match séquentiel (1 = premier match historique)
        df_all['id_match'] = range(1, len(df_all) + 1)
        
        # Réorganiser colonnes dans l'ordre final
        df_final = df_all[[
            'id_match', 'home_team', 'away_team', 'home_result', 'away_result',
            'result', 'date', 'round', 'city', 'edition'
        ]].copy()
        
        logger.info("✅ Consolidation terminée")
        return df_final
    
    def validate(self, df):
        """
        Validation finale des données
        """
        logger.info("✔️  Validation des données...")
        
        issues = []
        
        # Vérifier colonnes obligatoires
        required_cols = ['id_match', 'home_team', 'away_team', 'home_result', 
                        'away_result', 'result', 'date', 'round', 'city', 'edition']
        
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            issues.append(f"Colonnes manquantes: {missing_cols}")
        
        # Vérifier NULL
        null_counts = df[required_cols].isnull().sum()
        null_issues = null_counts[null_counts > 0]
        if not null_issues.empty:
            issues.append(f"Valeurs NULL détectées:\n{null_issues}")
        
        # Vérifier result values
        # Vérifier result values
        # Maintenant result contient soit:
        # - Le nom d'une équipe (gagnant)
        # - "draw" (match nul)
        # - None (erreur)

        # Vérifier qu'il n'y a pas de valeurs comme "home_team" ou "away_team"
        invalid_values = df[df['result'].isin(['home_team', 'away_team'])]
        if len(invalid_values) > 0:
            issues.append(f"Valeurs 'result' invalides (home_team/away_team): {len(invalid_values)} lignes")
            logger.warning(f"⚠️  Lignes avec result invalide:\n{invalid_values[['home_team', 'away_team', 'result']].head()}")

        # Vérifier que les matchs nuls sont bien marqués "draw"
        draw_matches = df[df['result'] == 'draw']
        draw_but_not_equal = draw_matches[draw_matches['home_result'] != draw_matches['away_result']]
        if len(draw_but_not_equal) > 0:
            issues.append(f"{len(draw_but_not_equal)} matchs marqués 'draw' mais avec scores différents")

        # Vérifier que les gagnants correspondent aux scores
        for idx, row in df.iterrows():
            if row['result'] != 'draw' and row['result'] is not None:
                # Vérifier que le gagnant correspond au score
                if row['result'] == row['home_team'] and row['home_result'] <= row['away_result']:
                    issues.append(f"Ligne {idx}: {row['home_team']} marqué gagnant mais {row['home_result']} ≤ {row['away_result']}")
                elif row['result'] == row['away_team'] and row['away_result'] <= row['home_result']:
                    issues.append(f"Ligne {idx}: {row['away_team']} marqué gagnant mais {row['away_result']} ≤ {row['home_result']}")
        
        # Vérifier id_match séquentiel
        expected_ids = set(range(1, len(df) + 1))
        actual_ids = set(df['id_match'])
        if expected_ids != actual_ids:
            issues.append("id_match non séquentiel ou avec gaps")
        
        # Vérifier doublons
        duplicates = df.duplicated(subset=['home_team', 'away_team', 'date']).sum()
        if duplicates > 0:
            issues.append(f"{duplicates} doublons détectés")
        
        # Rapport
        if issues:
            logger.warning("⚠️  Problèmes de validation détectés:")
            for issue in issues:
                logger.warning(f"  - {issue}")
            return False
        else:
            logger.info("✅ Validation réussie - Toutes les vérifications passées")
            return True
        
        # Stats (toujours afficher)
        logger.info(f"""
📊 Statistiques finales:
  - Total matchs: {len(df)}
  - Éditions: {df['edition'].nunique()} ({df['edition'].min()} - {df['edition'].max()})
  - Équipes uniques: {pd.concat([df['home_team'], df['away_team']]).nunique()}
  - Villes: {df['city'].nunique()}
  - Premier match (id=1): {df.iloc[0]['home_team']} vs {df.iloc[0]['away_team']} ({df.iloc[0]['date'].strftime('%Y-%m-%d')})
  - Dernier match (id={len(df)}): {df.iloc[-1]['home_team']} vs {df.iloc[-1]['away_team']} ({df.iloc[-1]['date'].strftime('%Y-%m-%d')})
        """)
        
        return True

    def analyze_results(self, df):
        """
        Analyse des résultats pour vérification
        """
        logger.info("📊 Analyse des résultats...")
        
        total_matches = len(df)
        draws = df[df['result'] == 'draw']
        home_wins = df[df['result'] == df['home_team']]
        away_wins = df[df['result'] == df['away_team']]
        
        logger.info(f"""
    📈 Statistiques des résultats:
    - Total matchs: {total_matches}
    - Victoires à domicile: {len(home_wins)} ({len(home_wins)/total_matches*100:.1f}%)
    - Victoires à l'extérieur: {len(away_wins)} ({len(away_wins)/total_matches*100:.1f}%)
    - Matchs nuls: {len(draws)} ({len(draws)/total_matches*100:.1f}%)
    
    🎯 Exemples de résultats:
    - Victoire domicile: {home_wins.iloc[0]['home_team']} {home_wins.iloc[0]['home_result']}-{home_wins.iloc[0]['away_result']} {home_wins.iloc[0]['away_team']} → {home_wins.iloc[0]['result']}
    - Victoire extérieur: {away_wins.iloc[0]['home_team']} {away_wins.iloc[0]['home_result']}-{away_wins.iloc[0]['away_result']} {away_wins.iloc[0]['away_team']} → {away_wins.iloc[0]['result']}
    - Match nul: {draws.iloc[0]['home_team']} {draws.iloc[0]['home_result']}-{draws.iloc[0]['away_result']} {draws.iloc[0]['away_team']} → {draws.iloc[0]['result']}
        """)
        
        # Vérifier les valeurs problématiques
        problematic = df[(df['result'] != 'draw') & 
                        (df['result'] != df['home_team']) & 
                        (df['result'] != df['away_team'])]
        
        if len(problematic) > 0:
            logger.warning(f"⚠️  {len(problematic)} résultats problématiques:")
            for idx, row in problematic.head(5).iterrows():
                logger.warning(f"  - Ligne {idx}: {row['home_team']} {row['home_result']}-{row['away_result']} {row['away_team']} → {row['result']}")
# =====================================================================
# CLASSE LOAD (Data Engineer 3)
# =====================================================================

class WorldCupLoader:
    """Chargement des données dans SQLite"""
    
    def __init__(self, db_path="data/worldcup.db"):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Connexion à la base SQLite"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Pour obtenir les résultats sous forme de dict
            logger.info(f"✅ Connexion à {self.db_path} établie")
        except Exception as e:
            logger.error(f"❌ Erreur connexion DB: {e}")
            raise
    
    def create_schema(self):
        """Création du schéma de la table principale"""
        logger.info("🏗️  Création du schéma principal...")
        
        create_table_sql = """
        -- Table principale des matchs
        DROP TABLE IF EXISTS world_cup_matches;
        
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
            -- Nouvelles colonnes optionnelles
            source TEXT,
            match_id_2018 INTEGER,
            stadium_id INTEGER,
            stadium_name TEXT,
            home_fifacode TEXT,
            away_fifacode TEXT
        );
        
        -- Index pour optimiser les requêtes
        CREATE INDEX idx_edition ON world_cup_matches(edition);
        CREATE INDEX idx_teams ON world_cup_matches(home_team, away_team);
        CREATE INDEX idx_date ON world_cup_matches(date);
        CREATE INDEX idx_round ON world_cup_matches(round);
        CREATE INDEX idx_city ON world_cup_matches(city);
        CREATE INDEX idx_result ON world_cup_matches(result);
        
        -- Table pour les stades (optionnel)
        DROP TABLE IF EXISTS stadiums;
        
        CREATE TABLE IF NOT EXISTS stadiums (
            stadium_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT NOT NULL,
            lat REAL,
            lng REAL,
            image TEXT
        );
        
        -- Table pour les équipes (optionnel)
        DROP TABLE IF EXISTS teams;
        
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            fifaCode TEXT,
            iso2 TEXT,
            flag TEXT,
            emojiString TEXT
        );
        
        -- Table pour les chaînes TV (optionnel)
        DROP TABLE IF EXISTS tv_channels;
        
        CREATE TABLE IF NOT EXISTS tv_channels (
            channel_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            icon TEXT,
            country TEXT,
            iso2 TEXT,
            languages TEXT
        );
        """
        
        try:
            self.conn.executescript(create_table_sql)
            self.conn.commit()
            logger.info("✅ Schéma principal créé avec succès")
        except Exception as e:
            logger.error(f"❌ Erreur création schéma: {e}")
            raise
    
    def load_data(self, df):
        """Chargement des données dans la table principale"""
        logger.info(f"📤 Chargement de {len(df)} matchs dans la table principale...")
        
        try:
            # Préparer les données pour SQLite
            df_to_load = df.copy()
            
            # Convertir date en string pour SQLite
            df_to_load['date'] = df_to_load['date'].dt.strftime('%Y-%m-%d')
            
            # S'assurer que toutes les colonnes optionnelles existent
            optional_columns = ['source', 'match_id_2018', 'stadium_id', 
                              'stadium_name', 'home_fifacode', 'away_fifacode']
            
            for col in optional_columns:
                if col not in df_to_load.columns:
                    df_to_load[col] = None
            
            # Sélectionner uniquement les colonnes qui existent dans le schéma
            table_columns = ['id_match', 'home_team', 'away_team', 'home_result', 'away_result',
                           'result', 'date', 'round', 'city', 'edition', 'source',
                           'match_id_2018', 'stadium_id', 'stadium_name',
                           'home_fifacode', 'away_fifacode']
            
            # Garder seulement les colonnes qui existent dans DataFrame
            available_columns = [col for col in table_columns if col in df_to_load.columns]
            df_to_load = df_to_load[available_columns]
            
            # Insert dans SQLite
            df_to_load.to_sql(
                'world_cup_matches',
                self.conn,
                if_exists='append',
                index=False
            )
            
            self.conn.commit()
            logger.info("✅ Données principales chargées avec succès")
            
        except Exception as e:
            logger.error(f"❌ Erreur chargement données principales: {e}")
            self.conn.rollback()
            raise
    
    def load_additional_data(self, json_data):
        """
        Charger les données supplémentaires (stades, équipes, chaînes)
        """
        logger.info("📤 Chargement des données supplémentaires...")
        
        try:
            # 1. Charger les stades
            if 'stadiums' in json_data and json_data['stadiums']:
                stadiums_df = pd.DataFrame(json_data['stadiums'])
                # Renommer pour correspondre au schéma
                stadiums_df = stadiums_df.rename(columns={'id': 'stadium_id'})
                stadiums_df.to_sql('stadiums', self.conn, if_exists='replace', index=False)
                logger.info(f"✅ {len(stadiums_df)} stades chargés")
            
            # 2. Charger les équipes
            if 'teams' in json_data and json_data['teams']:
                teams_df = pd.DataFrame(json_data['teams'])
                # Renommer pour correspondre au schéma
                teams_df = teams_df.rename(columns={'id': 'team_id'})
                # Convertir la liste de langues en string
                if 'lang' in teams_df.columns:
                    teams_df['languages'] = teams_df['lang'].apply(lambda x: ','.join(x) if isinstance(x, list) else str(x))
                    teams_df = teams_df.drop('lang', axis=1)
                teams_df.to_sql('teams', self.conn, if_exists='replace', index=False)
                logger.info(f"✅ {len(teams_df)} équipes chargées")
            
            # 3. Charger les chaînes TV
            if 'tvchannels' in json_data and json_data['tvchannels']:
                channels_df = pd.DataFrame(json_data['tvchannels'])
                # Renommer pour correspondre au schéma
                channels_df = channels_df.rename(columns={'id': 'channel_id'})
                # Convertir la liste de langues en string
                if 'lang' in channels_df.columns:
                    channels_df['languages'] = channels_df['lang'].apply(lambda x: ','.join(x) if isinstance(x, list) else str(x))
                    channels_df = channels_df.drop('lang', axis=1)
                channels_df.to_sql('tv_channels', self.conn, if_exists='replace', index=False)
                logger.info(f"✅ {len(channels_df)} chaînes TV chargées")
            
            self.conn.commit()
            logger.info("✅ Données supplémentaires chargées avec succès")
            
        except Exception as e:
            logger.error(f"❌ Erreur chargement données supplémentaires: {e}")
            self.conn.rollback()
            raise
    
    def verify_load(self):
        """Vérification du chargement"""
        logger.info("🔍 Vérification du chargement...")
        
        try:
            # 1. Vérifier la table principale
            query = "SELECT COUNT(*) as total FROM world_cup_matches"
            result = pd.read_sql_query(query, self.conn)
            total_matches = result['total'][0]
            logger.info(f"✅ {total_matches} matchs en base")
            
            # 2. Statistiques par édition
            query_editions = """
            SELECT edition, COUNT(*) as matchs
            FROM world_cup_matches
            GROUP BY edition
            ORDER BY edition
            """
            editions_df = pd.read_sql_query(query_editions, self.conn)
            logger.info("📊 Répartition par édition:")
            for _, row in editions_df.iterrows():
                logger.info(f"  - {row['edition']}: {row['matchs']} matchs")
            
            # 3. Premier et dernier match
            query_first = """
            SELECT id_match, home_team, away_team, date, edition 
            FROM world_cup_matches 
            WHERE id_match = 1
            """
            first = pd.read_sql_query(query_first, self.conn)
            if len(first) > 0:
                logger.info(f"🥇 Premier match (id=1): {first.iloc[0]['home_team']} vs {first.iloc[0]['away_team']} ({first.iloc[0]['date']}) - {first.iloc[0]['edition']}")
            
            query_last = """
            SELECT id_match, home_team, away_team, date, edition 
            FROM world_cup_matches 
            ORDER BY id_match DESC 
            LIMIT 1
            """
            last = pd.read_sql_query(query_last, self.conn)
            if len(last) > 0:
                logger.info(f"🏆 Dernier match (id={last.iloc[0]['id_match']}): {last.iloc[0]['home_team']} vs {last.iloc[0]['away_team']} ({last.iloc[0]['date']}) - {last.iloc[0]['edition']}")
            
            # 4. Vérifier les tables supplémentaires
            for table in ['stadiums', 'teams', 'tv_channels']:
                try:
                    query = f"SELECT COUNT(*) as count FROM {table}"
                    result = pd.read_sql_query(query, self.conn)
                    if result['count'][0] > 0:
                        logger.info(f"✅ Table {table}: {result['count'][0]} enregistrements")
                    else:
                        logger.warning(f"⚠️  Table {table}: vide")
                except:
                    logger.debug(f"ℹ️  Table {table} non trouvée (optionnel)")
            
            # 5. Statistiques générales
            query_stats = """
            SELECT 
                COUNT(DISTINCT home_team || away_team) as matchs_uniques,
                COUNT(DISTINCT home_team) as equipes_domicile,
                COUNT(DISTINCT away_team) as equipes_exterieur,
                MIN(date) as date_debut,
                MAX(date) as date_fin
            FROM world_cup_matches
            """
            stats = pd.read_sql_query(query_stats, self.conn)
            if len(stats) > 0:
                logger.info(f"""
📈 Statistiques globales:
  - Matchs uniques: {stats.iloc[0]['matchs_uniques']}
  - Équipes domicile uniques: {stats.iloc[0]['equipes_domicile']}
  - Équipes extérieur uniques: {stats.iloc[0]['equipes_exterieur']}
  - Période couverte: {stats.iloc[0]['date_debut']} au {stats.iloc[0]['date_fin']}
                """)
            
        except Exception as e:
            logger.error(f"❌ Erreur vérification: {e}")
            raise
    
    def clean_database(self):
        """Nettoyer complètement la base de données"""
        logger.info("🧹 Nettoyage complet de la base de données...")
        
        drop_all_tables = """
        DROP TABLE IF EXISTS world_cup_matches;
        DROP TABLE IF EXISTS stadiums;
        DROP TABLE IF EXISTS teams;
        DROP TABLE IF EXISTS tv_channels;
        """
        
        try:
            self.conn.executescript(drop_all_tables)
            self.conn.commit()
            logger.info("✅ Base de données nettoyée")
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage base: {e}")
            raise

    def close(self):
        """Fermeture connexion"""
        if self.conn:
            self.conn.close()
            logger.info("✅ Connexion fermée")
    
        """Fermeture connexion"""
        if self.conn:
            self.conn.close()
            logger.info("✅ Connexion fermée")


# =====================================================================
# PIPELINE PRINCIPAL (Chef de Projet)
# =====================================================================

def run_etl_pipeline():
    """
    Pipeline ETL complet
    """
    logger.info("="*70)
    logger.info("🚀 DÉBUT DU PIPELINE ETL - FIFA WORLD CUP")
    logger.info("="*70)
    
    try:
        # -------------------- EXTRACT --------------------
        logger.info("\n📥 PHASE 1: EXTRACTION")
        logger.info("-" * 50)
        
        extractor = WorldCupExtractor(data_dir="data/raw")
        
        df_source1 = extractor.extract_source1("matches_1930-2010.csv")
        df_source2 = extractor.extract_source2("WorldCupMatches2014.csv")
        df_source3 = extractor.extract_source3("Fifa_world_cup_matches.csv")
        json_source4 = extractor.extract_source4("data_2018.json")
        
        # -------------------- TRANSFORM --------------------
        logger.info("\n🔄 PHASE 2: TRANSFORMATION")
        logger.info("-" * 50)
        
        transformer = WorldCupTransformer()
        
        df_clean1 = transformer.transform_source1(df_source1)
        df_clean2 = transformer.transform_source2(df_source2)
        df_clean3 = transformer.transform_source3(df_source3)
        df_clean4 = transformer.transform_source4(json_source4)
        
        # Enrichissement optionnel des données 2018
        if len(df_clean4) > 0:
            logger.info("\n🌟 Enrichissement des données 2018...")
            df_clean4_enriched = transformer.enrich_with_stadiums(df_clean4, json_source4)
            df_clean4_enriched = transformer.enrich_with_teams_info(df_clean4_enriched, json_source4)
            
            # Sauvegarder les données enrichies séparément
            df_clean4_enriched.to_csv("data/processed/worldcup_2018_enriched.csv", index=False)
            logger.info("💾 Données 2018 enrichies sauvegardées: data/processed/worldcup_2018_enriched.csv")
            
            # Utiliser les données enrichies pour la consolidation
            df_clean4 = df_clean4_enriched
        
        # Consolidation avec les 4 sources
        df_final = transformer.consolidate([df_clean1, df_clean2, df_clean3, df_clean4])
        
        # Analyse des résultats (NOUVEAU)
        transformer.analyze_results(df_final)
        # Validation
        is_valid = transformer.validate(df_final)
        
        if not is_valid:
            logger.warning("⚠️  Des problèmes de validation ont été détectés, mais le pipeline continue")
        
        # -------------------- LOAD --------------------
        logger.info("\n📤 PHASE 3: CHARGEMENT")
        logger.info("-" * 50)
        
        loader = WorldCupLoader(db_path="data/worldcup.db")
        loader.connect()
        # Nettoyer la base de données
        loader.clean_database()
        loader.create_schema()
        
        # 1. Charger les données principales
        loader.load_data(df_final)
        
        # 2. Charger les données supplémentaires (stades, équipes, chaînes)
        loader.load_additional_data(json_source4)
        
        # 3. Vérifier le chargement
        loader.verify_load()
        loader.close()
        
        # -------------------- SUCCÈS --------------------
        logger.info("\n" + "="*70)
        logger.info("✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS")
        logger.info("="*70)
        logger.info(f"""
📊 Résumé:
  - Sources traitées: 4/4 (CSV 1930-2010 + CSV 2014 + CSV FIFA + JSON 2018)
  - Total matchs chargés: {len(df_final)}
  - Base de données: data/worldcup.db
  - Période couverte: {df_final['date'].min().year} - {df_final['date'].max().year}
  - Équipes uniques: {pd.concat([df_final['home_team'], df_final['away_team']]).nunique()}
  - Tables créées: world_cup_matches, stadiums, teams, tv_channels
  
🎯 Points forts:
  ✓ Données historiques complètes (1930-2022)
  ✓ Édition 2018 avec stades et équipes détaillés
  ✓ Schéma relationnel complet
  ✓ Validation rigoureuse
  ✓ Index pour performances
  
📈 Accès aux données:
  - Fichier CSV: data/processed/worldcup_clean.csv
  - Base SQLite: data/worldcup.db
  - Données 2018 enrichies: data/processed/worldcup_2018_enriched.csv
  
🔍 Exemples de requêtes SQL:
  1. SELECT * FROM world_cup_matches WHERE edition = '2018';
  2. SELECT round, COUNT(*) FROM world_cup_matches GROUP BY round;
  3. SELECT * FROM stadiums WHERE city = 'Moscow';
  4. SELECT team.* FROM teams team JOIN world_cup_matches m ON team.name = m.home_team;
        """)
        
        return df_final
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR PIPELINE: {e}")
        import traceback
        traceback.print_exc()
   


# =====================================================================
# POINT D'ENTRÉE
# =====================================================================

if __name__ == "__main__":
    # Créer répertoires si nécessaire
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    
    # Lancer le pipeline
    df_result = run_etl_pipeline()
    
    # Sauvegarder aussi en CSV pour inspection
    df_result.to_csv("data/processed/worldcup_clean.csv", index=False)
    logger.info("💾 Données sauvegardées aussi en CSV: data/processed/worldcup_clean.csv")