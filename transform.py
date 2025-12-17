import pandas as pd
import re
import logging
from config import TEAMS_MAPPING, CITIES_MAPPING, ROUNDS_MAPPING, TEAMS_MAPPING_2018, STADIUMS_MAPPING_2018

logger = logging.getLogger(__name__)

class WorldCupTransformer:
    """
    Module de Transformation ETL.
    Responsable du nettoyage, de la normalisation et de l'enrichissement des données 
    provenant de sources hétérogènes.
    """
    
    def __init__(self):
        self.teams_mapping = TEAMS_MAPPING
        self.cities_mapping = CITIES_MAPPING
        self.rounds_mapping = ROUNDS_MAPPING
        self.stadiums_mapping = STADIUMS_MAPPING_2018
        self.teams_mapping_2018 = TEAMS_MAPPING_2018
        
    @staticmethod
    def parse_score(score_str):
        """
        Extrait les scores numériques depuis des chaînes non structurées.
        Retourne : (home, away) ou (None, None).
        """
        if pd.isna(score_str) or score_str is None:
            return None, None
        
        try:
            score_clean = str(score_str).strip()
            
            if isinstance(score_str, (tuple, list)) and len(score_str) == 2:
                return int(score_str[0]), int(score_str[1])
            
            match = re.match(r'(\d+)\s*-\s*(\d+)', score_clean)
            if match:
                return int(match.group(1)), int(match.group(2))
            
            if ':' in score_clean:
                parts = score_clean.split(':')
                if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
                    return int(parts[0]), int(parts[1])
            
            return None, None
        except Exception:
            return None, None

    def normalize_team(self, team_name):
        """Standardise les noms d'équipes."""
        if pd.isna(team_name): return "Unknown"
        
        try:
            team = str(team_name).strip()
        except:
            return "Unknown"
        
        if team.isdigit(): return "Unknown"
        
        # Nettoyage syntaxique
        team = re.sub(r'\s*\(.*\)', '', team) # Enlève les parenthèses
        team = team.replace('"', '').strip()
        
        # 1. Gestion spécifique des artefacts d'encodage du fichier TXT
        # Pour : C_¯_¿_e d'Ivoire
        if "C_" in team and "Ivoire" in team:
            return "Cote d'Ivoire"
        if "Trinidad" in team and "Tobago" in team:
            return "Trinidad and Tobago"
            
        # 2. Mapping
        if team in self.teams_mapping:
            return self.teams_mapping[team]
        
        if team.title() in self.teams_mapping:
            return self.teams_mapping[team.title()]
        
        # 3. Correction générique encodage
        if "CTe" in team or "Côte" in team or "Cote" in team:
            return "Cote d'Ivoire"
            
        return team.title()

    def normalize_city(self, city_name):
        """Normalise les noms de villes."""
        if pd.isna(city_name): return None
        city = str(city_name).strip().replace('"', '')
        city = re.sub(r'\([^)]*\)', '', city).strip()
        if city in self.cities_mapping:
            city = self.cities_mapping[city]
        return city.title()
    
    def normalize_round(self, round_str):
        """Harmonise les phases de tournoi."""
        if pd.isna(round_str): return None
        round_clean = str(round_str).strip().replace('"', '')
        if round_clean in self.rounds_mapping:
            return self.rounds_mapping[round_clean]
        if 'group' in round_clean.lower() or 'poule' in round_clean.lower():
            return "Group Stage"
        return round_clean.title()
    
    @staticmethod
    def compute_result(home_goals, away_goals, home_team=None, away_team=None):
        """Détermine le résultat (Gagnant ou 'draw')."""
        if pd.isna(home_goals) or pd.isna(away_goals): return None
        if home_goals > away_goals:
            return f"{home_team}" if home_team else "home_team"
        elif away_goals > home_goals:
            return f"{away_team}" if away_team else "away_team"
        else:
            return "draw"
    
    @staticmethod
    def parse_datetime(datetime_str):
        """Parse les dates hétérogènes."""
        if pd.isna(datetime_str): return None
        datetime_str = str(datetime_str).strip().replace('"', '')
        try:
            if ' - ' in datetime_str:
                date_part = datetime_str.split(' - ')[0].strip()
                return pd.to_datetime(date_part, format='%d %b %Y')
            if len(datetime_str.split()) == 3:
                return pd.to_datetime(datetime_str, format='%d %b %Y')
            return pd.to_datetime(datetime_str)
        except Exception:
            return None
    
    def transform_source1(self, df):
        """
        Traite le fichier historique (1930-2010).
        CORRECTION IMPORTANTE : Force l'édition (année) en format entier texte (ex: "1930")
        pour garantir la compatibilité avec le fichier de dates.
        """
        logger.info("🔄 Transformation Source 1 (1930-2010)...")
        
        if 'year' in df.columns:
            df = df[df['year'] != 2014]

        df_clean = df.copy()
        
        # Détection colonnes
        team1_cols = [col for col in df_clean.columns if 'team1' in col.lower() or 'home' in col.lower()]
        team2_cols = [col for col in df_clean.columns if 'team2' in col.lower() or 'away' in col.lower()]
        col_t1 = team1_cols[0] if team1_cols else df_clean.columns[3]
        col_t2 = team2_cols[0] if team2_cols else df_clean.columns[4]
        
        # Parsing scores
        scores = df_clean['score'].apply(self.parse_score)
        df_clean['home_result'] = scores.apply(lambda x: x[0])
        df_clean['away_result'] = scores.apply(lambda x: x[1])
        
        # Gestion des erreurs de parsing (DQ)
        parsing_errors = df_clean[df_clean['home_result'].isna()]
        if len(parsing_errors) > 0:
            logger.warning(f"⚠️  SCORES NON PARSABLES : {len(parsing_errors)} matchs -> mis à 0-0")
            df_clean['home_result'] = df_clean['home_result'].fillna(0)
            df_clean['away_result'] = df_clean['away_result'].fillna(0)

        df_clean['home_result'] = df_clean['home_result'].astype(int)
        df_clean['away_result'] = df_clean['away_result'].astype(int)
        
        df_clean['home_team'] = df_clean[col_t1].apply(self.normalize_team)
        df_clean['away_team'] = df_clean[col_t2].apply(self.normalize_team)
        
        venue_cols = [col for col in df_clean.columns if 'venue' in col.lower() or 'city' in col.lower()]
        df_clean['city'] = df_clean[venue_cols[0] if venue_cols else df_clean.columns[6]].apply(self.normalize_city)
        
        # --- CORRECTION DATE & EDITION ---
        year_cols = [col for col in df_clean.columns if 'year' in col.lower()]
        if year_cols:
            # On force la conversion en numérique, puis entier, puis string
            # Cela évite "1930.0" qui ne matcherait pas "1930"
            df_clean['edition'] = pd.to_numeric(df_clean[year_cols[0]], errors='coerce').fillna(0).astype(int).astype(str)
            
            # Date par défaut (1er Juillet) en attendant l'enrichissement
            df_clean['date'] = df_clean['edition'].apply(lambda y: pd.to_datetime(f"{y}-07-01") if y != "0" else None)
        else:
            logger.warning("⚠️  Colonne 'year' non trouvée")
            df_clean['edition'] = 'Unknown'
            df_clean['date'] = None
            
        round_cols = [col for col in df_clean.columns if 'round' in col.lower()]
        df_clean['round'] = df_clean[round_cols[0] if round_cols else df_clean.columns[1]].apply(self.normalize_round)
        
        df_clean['result'] = df_clean.apply(
            lambda row: self.compute_result(row['home_result'], row['away_result'], row['home_team'], row['away_team']), axis=1
        )
        
        return df_clean[['home_team', 'away_team', 'home_result', 'away_result', 'result', 'date', 'round', 'city', 'edition']].copy()

    def enrich_with_historical_dates(self, df_matches, df_dates):
        """
        Enrichissement optimisé pour le fichier TXT (Dates ISO).
        """
        logger.info("📅 Enrichissement avec les dates exactes historiques (Source TXT)...")
        
        if df_dates is None or df_dates.empty:
            return df_matches

        df_main = df_matches.copy()
        df_dates_clean = df_dates.copy()

        # 1. PARSING DATE SIMPLIFIÉ (Format ISO YYYY-MM-DD confirmé)
        df_dates_clean['date_exacte'] = pd.to_datetime(df_dates_clean['date_exacte'], format='%Y-%m-%d', errors='coerce')
        
        # Vérification
        valid_dates = df_dates_clean['date_exacte'].notna().sum()
        if valid_dates == 0:
            logger.error("❌ ERREUR : Aucune date valide trouvée. Vérifiez que le format est bien AAAA-MM-JJ.")
            return df_matches

        # 2. Construction Dictionnaire
        date_lookup = {}
        for _, row in df_dates_clean.iterrows():
            if pd.isna(row['date_exacte']): continue
            
            t1 = self.normalize_team(row['home_team']).strip()
            t2 = self.normalize_team(row['away_team']).strip()
            year = str(row['date_exacte'].year)
            
            date_lookup[(t1, t2, year)] = row['date_exacte']
            date_lookup[(t2, t1, year)] = row['date_exacte']

        # 3. Recherche
        missed_matches = []

        def find_exact_date(row):
            # On ignore les matchs récents (> 2010)
            try:
                if int(row['edition']) > 2010: return row['date']
            except: pass

            t1 = str(row['home_team']).strip()
            t2 = str(row['away_team']).strip()
            year = str(row['edition'])
            
            if (t1, t2, year) in date_lookup:
                return date_lookup[(t1, t2, year)]
            
            if len(missed_matches) < 5 and year == '1930':
                missed_matches.append(f"Cherché: '{t1}' vs '{t2}' ({year})")
            
            return row['date']

        df_main['date_new'] = df_main.apply(find_exact_date, axis=1)
        
        changes = (df_main['date'].astype(str) != df_main['date_new'].astype(str)).sum()
        
        df_main['date'] = df_main['date_new']
        df_main = df_main.drop(columns=['date_new'])
        
        logger.info(f"✅ Dates mises à jour : {changes} matchs.")
        
        if changes < 100:
            logger.warning("⚠️  Echecs fréquents (Top 5 en 1930) :")
            for msg in missed_matches:
                logger.warning(f"   ❌ {msg}")

        return df_main
        
    def transform_source2(self, df):
        logger.info("🔄 Transformation Source 2 (2014)...")
        df_clean = df.copy()
        df_clean['home_result'] = pd.to_numeric(df_clean.get('Home Team Goals'), errors='coerce').fillna(0).astype(int)
        df_clean['away_result'] = pd.to_numeric(df_clean.get('Away Team Goals'), errors='coerce').fillna(0).astype(int)
        
        if 'Home Team Name' in df_clean.columns:
            df_clean['home_team'] = df_clean['Home Team Name'].apply(self.normalize_team)
            df_clean['away_team'] = df_clean['Away Team Name'].apply(self.normalize_team)
        
        df_clean['result'] = df_clean.apply(lambda row: self.compute_result(row['home_result'], row['away_result'], row['home_team'], row['away_team']), axis=1)
        df_clean['city'] = df_clean.get('City', 'Unknown').apply(self.normalize_city) if 'City' in df_clean.columns else 'Unknown'
        df_clean['round'] = df_clean.get('Stage', 'Group Stage').apply(self.normalize_round) if 'Stage' in df_clean.columns else 'Group Stage'
        df_clean['edition'] = df_clean.get('Year', '2014').astype(str) if 'Year' in df_clean.columns else '2014'
        
        if 'Datetime' in df_clean.columns:
            df_clean['date'] = df_clean['Datetime'].apply(self.parse_datetime)
        else:
            df_clean['date'] = pd.to_datetime('2014-07-01')

        return df_clean[['home_team', 'away_team', 'home_result', 'away_result', 'result', 'date', 'round', 'city', 'edition']].copy()

    def transform_source3(self, df):
        logger.info("🔄 Transformation Source 3 (Fifa_world_cup_matches)...")
        df_clean = df.copy()
        col_map = {'home_team': 'team1', 'away_team': 'team2'}
        for col in df_clean.columns:
            if 'number of goals team1' in col.lower(): col_map['home_goals'] = col
            elif 'number of goals team2' in col.lower(): col_map['away_goals'] = col
            elif 'date' in col.lower(): col_map['date'] = col
            elif 'year' in col.lower(): col_map['year'] = col
            elif 'city' in col.lower(): col_map['city'] = col
            elif 'round' in col.lower(): col_map['round'] = col

        result_df = pd.DataFrame()
        result_df['home_team'] = df_clean[col_map['home_team']].apply(self.normalize_team)
        result_df['away_team'] = df_clean[col_map['away_team']].apply(self.normalize_team)
        result_df['home_result'] = pd.to_numeric(df_clean[col_map['home_goals']], errors='coerce').fillna(0).astype(int)
        result_df['away_result'] = pd.to_numeric(df_clean[col_map['away_goals']], errors='coerce').fillna(0).astype(int)
        result_df['result'] = result_df.apply(lambda row: self.compute_result(row['home_result'], row['away_result'], row['home_team'], row['away_team']), axis=1)
        
        def parse_date_special(date_str):
            if pd.isna(date_str): return None
            s = str(date_str).strip()
            if len(s) == 7 and s[2:5].isalpha():
                try: return pd.to_datetime(f"{s[:2]} {s[2:5]} 20{s[5:]}", format='%d %b %Y')
                except: pass
            return self.parse_datetime(s)

        if 'date' in col_map: result_df['date'] = df_clean[col_map['date']].apply(parse_date_special)
        else: result_df['date'] = None
            
        result_df['edition'] = df_clean[col_map['year']].astype(str) if 'year' in col_map else '2022'
        result_df['city'] = df_clean[col_map['city']].apply(self.normalize_city) if 'city' in col_map else 'Unknown'
        result_df['round'] = df_clean[col_map['round']].apply(self.normalize_round) if 'round' in col_map else 'Group Stage'
        if result_df['date'].isnull().any(): result_df['date'] = result_df['date'].fillna(pd.to_datetime('1900-01-01'))
            
        return result_df

    def transform_source4(self, json_data):
        logger.info("🔄 Transformation Source 4 (2018)...")
        if not json_data: return pd.DataFrame()
        matches_list = []
        for g, d in json_data.get('groups', {}).items():
            for m in d.get('matches', []):
                m['type'] = 'group'; m['group'] = g; matches_list.append(m)
        for s, d in json_data.get('knockout', {}).items():
            for m in d.get('matches', []):
                m['type'] = 'knockout'; m['round_raw'] = s; matches_list.append(m)
        
        final_list = []
        for m in matches_list:
            home = self.teams_mapping_2018.get(m.get('home_team'), f"Unknown_{m.get('home_team')}")
            away = self.teams_mapping_2018.get(m.get('away_team'), f"Unknown_{m.get('away_team')}")
            home_n, away_n = self.normalize_team(home), self.normalize_team(away)
            d_obj = pd.to_datetime(m.get('date', '').split('T')[0]) if m.get('date') else pd.to_datetime('2018-07-01')
            s_id = m.get('stadium')
            city = next((s['city'] for s in json_data.get('stadiums', []) if s['id'] == s_id), "Unknown")
            
            final_list.append({
                'home_team': home_n, 'away_team': away_n,
                'home_result': m.get('home_result', 0), 'away_result': m.get('away_result', 0),
                'result': self.compute_result(m.get('home_result', 0), m.get('away_result', 0), home_n, away_n),
                'date': d_obj, 'round': self.normalize_round(m.get('round_raw', 'Group Stage')) if m['type'] == 'knockout' else 'Group Stage',
                'city': self.normalize_city(city), 'edition': '2018', 'source': 'json_2018', 'stadium_id': s_id
            })
        return pd.DataFrame(final_list)

    def enrich_2022_with_cities(self, df_2022, df_cities):
        logger.info("🏙️  Correction des villes 2022...")
        if df_cities is None or df_cities.empty: return df_2022
        df_main = df_2022.copy()
        city_lookup = {}
        for _, row in df_cities.iterrows():
            t1, t2 = self.normalize_team(row['home_team']), self.normalize_team(row['away_team'])
            city_lookup[(t1, t2)] = self.normalize_city(row['city'])

        def find_city(row):
            if row['city'] != 'Unknown' and pd.notna(row['city']): return row['city']
            t1, t2 = self.normalize_team(row['home_team']), self.normalize_team(row['away_team'])
            if (t1, t2) in city_lookup: return city_lookup[(t1, t2)]
            if (t2, t1) in city_lookup and sorted([t1, t2]) != ['Croatia', 'Morocco']: return city_lookup[(t2, t1)]
            return row['city']

        df_main['city'] = df_main.apply(find_city, axis=1)
        return df_main

    def enrich_with_stadiums(self, df_2018, json_data):
        if 'stadiums' not in json_data: return df_2018
        stadiums_df = pd.DataFrame(json_data['stadiums']).rename(columns={'id': 'stadium_id', 'name': 'stadium_name'})
        return pd.merge(df_2018, stadiums_df[['stadium_id', 'stadium_name']], on='stadium_id', how='left') if 'stadium_id' in df_2018.columns else df_2018

    def enrich_with_teams_info(self, df_2018, json_data):
        return df_2018

    def consolidate(self, dfs_list):
        logger.info("🔗 Consolidation des sources...")
        df_all = pd.concat([df for df in dfs_list if df is not None], ignore_index=True)
        df_all = df_all.dropna(subset=['home_team', 'away_team', 'date'])
        df_all = df_all.drop_duplicates(subset=['home_team', 'away_team', 'date'], keep='first')
        df_all['date'] = pd.to_datetime(df_all['date'])
        df_all = df_all.sort_values('date').reset_index(drop=True)
        df_all['id_match'] = range(1, len(df_all) + 1)
        
        cols = ['id_match', 'home_team', 'away_team', 'home_result', 'away_result', 'result', 'date', 'round', 'city', 'edition']
        for c in ['stadium_id', 'stadium_name', 'source', 'match_id_2018']:
            if c in df_all.columns: cols.append(c)
        return df_all[cols].copy()

    def validate(self, df):
        required = ['id_match', 'home_team', 'away_team', 'home_result', 'away_result', 'result', 'date', 'round', 'city', 'edition']
        if set(required) - set(df.columns): return False
        return True
        
    def analyze_results(self, df):
        draws = df[df['result'] == 'draw']
        logger.info(f"📊 Analyse: {len(df)} matchs, {len(draws)} nuls.")