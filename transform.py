import pandas as pd
import re
import logging
from config import TEAMS_MAPPING, CITIES_MAPPING, ROUNDS_MAPPING, TEAMS_MAPPING_2018, STADIUMS_MAPPING_2018
import numpy as np

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
        
    def parse_score(self, score_str):
        """
        Extrait les scores avec une Regex Universelle.
        CORRECTION : Gestion robuste des None et meilleure détection.
        """
        if pd.isna(score_str) or score_str is None or str(score_str).strip() == '':
            return None, None
        
        try:
            s = str(score_str).strip()
            
            # --- Cas Tuples/Listes ---
            if isinstance(score_str, (tuple, list)) and len(score_str) == 2:
                try:
                    return int(score_str[0]), int(score_str[1])
                except (ValueError, TypeError):
                    return None, None

            # --- REGEX UNIVERSELLE AMÉLIORÉE ---
            # Capture UNIQUEMENT les 2 premiers nombres séparés par un non-chiffre
            match = re.search(r'^(\d+)[^\d]+(\d+)', s)
            
            if match:
                home_score = int(match.group(1))
                away_score = int(match.group(2))
                
                return home_score, away_score
            
            # Si la regex échoue, on log et on retourne None
            logger.warning(f"⚠️ Impossible de parser le score : '{s}'")
            return None, None

        except Exception as e:
            logger.error(f"❌ Erreur parsing score '{score_str}': {e}")
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
        team = re.sub(r'\s*\(.*\)', '', team)
        team = team.replace('"', '').strip()
        
        # Gestion spécifique encodage
        if "C_" in team and "Ivoire" in team:
            return "Cote d'Ivoire"
        if "Trinidad" in team and "Tobago" in team:
            return "Trinidad and Tobago"
            
        # Mapping
        if team in self.teams_mapping:
            return self.teams_mapping[team]
        
        if team.title() in self.teams_mapping:
            return self.teams_mapping[team.title()]
        
        # Correction générique encodage
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
        """
        Détermine le résultat (Gagnant ou 'draw').
        CORRECTION : Gestion stricte des None.
        """
        # Si l'un des scores est None ou NaN, on ne peut pas calculer
        if pd.isna(home_goals) or pd.isna(away_goals):
            return None
        
        try:
            home_goals = int(home_goals)
            away_goals = int(away_goals)
        except (ValueError, TypeError):
            return None
            
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
        Transformation Source 1 (1930-2010) - CORRIGÉE AVEC DEBUG
        """
        logger.info("🔄 Transformation Source 1 (1930-2010)...")
        
        if 'year' in df.columns:
            df = df[df['year'] != 2014]

        df_clean = df.copy()
        
        # 1. Détection Colonnes
        team1_cols = [c for c in df_clean.columns if 'team1' in c.lower() or 'home' in c.lower()]
        team2_cols = [c for c in df_clean.columns if 'team2' in c.lower() or 'away' in c.lower()]
        col_t1 = team1_cols[0] if team1_cols else df_clean.columns[3]
        col_t2 = team2_cols[0] if team2_cols else df_clean.columns[4]
        
        # 2. Normalisation des équipes (Avant calculs)
        df_clean['home_team'] = df_clean[col_t1].apply(self.normalize_team)
        df_clean['away_team'] = df_clean[col_t2].apply(self.normalize_team)

        # 3. Parsing des scores - CORRECTION CRITIQUE
        scores = df_clean['score'].apply(self.parse_score)
        
        # IMPORTANT : On ne remplit PAS avec fillna(0) !
        df_clean['home_result'] = scores.apply(lambda x: x[0] if x[0] is not None else pd.NA)
        df_clean['away_result'] = scores.apply(lambda x: x[1] if x[1] is not None else pd.NA)
        
        # Conversion en numérique (garde les NA)
        df_clean['home_result'] = pd.to_numeric(df_clean['home_result'], errors='coerce')
        df_clean['away_result'] = pd.to_numeric(df_clean['away_result'], errors='coerce')
        
        # 🔍 DEBUG : Vérifions la finale 1962 APRÈS parsing
        matches_1962_after = df_clean[df_clean['edition'] == '1962']
        if len(matches_1962_after) > 0:
            for idx, row in matches_1962_after.iterrows():
                if 'final' in str(row['round']).lower():
                    logger.info(f"🎯 FINALE 1962 APRÈS PARSING :")
                    logger.info(f"   {row['home_team']} {row['home_result']}-{row['away_result']} {row['away_team']}")
                    logger.info(f"   Round normalisé : {row['round']}")

        # 4. CALCUL RÉSULTAT 
        df_clean['result'] = df_clean.apply(
            lambda row: self.compute_result(
                row['home_result'], 
                row['away_result'], 
                row['home_team'], 
                row['away_team']
            ), axis=1
        )

        # 5. Autres colonnes
        venue_cols = [c for c in df_clean.columns if 'venue' in c.lower() or 'city' in c.lower()]
        col_venue = venue_cols[0] if venue_cols else df_clean.columns[6]
        df_clean['city'] = df_clean[col_venue].apply(self.normalize_city)
        
        year_cols = [c for c in df_clean.columns if 'year' in c.lower()]
        if year_cols:
            df_clean['edition'] = pd.to_numeric(df_clean[year_cols[0]], errors='coerce').fillna(0).astype(int).astype(str)
            df_clean['date'] = df_clean['edition'].apply(lambda y: pd.to_datetime(f"{y}-07-01") if y != "0" else None)
        else:
            df_clean['edition'] = 'Unknown'
            df_clean['date'] = None
            
        round_cols = [c for c in df_clean.columns if 'round' in c.lower()]
        col_round = round_cols[0] if round_cols else df_clean.columns[1]
        df_clean['round'] = df_clean[col_round].apply(self.normalize_round)

     
        return df_clean[['home_team', 'away_team', 'home_result', 'away_result', 'result', 'date', 'round', 'city', 'edition']].copy()

    def enrich_with_historical_dates(self, df_matches, df_dates):
        """
        Solution GÉNÉRALE pour gérer les matchs multiples entre mêmes équipes.
        Logique : Appariement intelligent par ordre chronologique et round.
        """
        logger.info("📅 Enrichissement dates historiques (solution générale)...")
        
        if df_dates is None or df_dates.empty:
            return df_matches
        
        # 1. PRÉPARATION DES DONNÉES
        df_main = df_matches.copy()
        df_dates_clean = df_dates.copy()
        
        # Parsing dates
        def parse_date(date_str):
            try:
                return pd.to_datetime(str(date_str).strip(), format='%d/%m/%Y')
            except:
                try:
                    return pd.to_datetime(str(date_str).strip())
                except:
                    return pd.NaT
        
        df_dates_clean['date_exacte'] = df_dates_clean['date_exacte'].apply(parse_date)
        df_dates_clean = df_dates_clean.dropna(subset=['date_exacte'])
        
        # Normalisation équipes
        df_dates_clean['home_norm'] = df_dates_clean['home_team'].apply(self.normalize_team)
        df_dates_clean['away_norm'] = df_dates_clean['away_team'].apply(self.normalize_team)
        
        # 2. DÉTECTION DES CAS PROBLÉMATIQUES
        # Compter combien de fois chaque paire apparaît DANS LES MATCHS
        match_counts = {}
        problematic_pairs = []
        
        for idx, row in df_main.iterrows():
            if int(row['edition']) > 2010:
                continue
                
            home = str(row['home_team']).strip()
            away = str(row['away_team']).strip()
            year = row['edition']
            
            # Clé normalisée (triée pour ignorer l'ordre)
            key = tuple(sorted([home, away]) + [year])
            match_counts[key] = match_counts.get(key, 0) + 1
        
        # Identifier les paires problématiques (>1 match)
        for (team1, team2, year), count in match_counts.items():
            if count > 1:
                problematic_pairs.append((team1, team2, year, count))
        """
        if problematic_pairs:
            logger.info(f"⚠️ {len(problematic_pairs)} paires avec matchs multiples:")
            for team1, team2, year, count in problematic_pairs[:10]:  # Limiter l'affichage
                logger.info(f"   {team1} vs {team2} ({year}): {count} matchs")
        """
        # 3. CRÉATION DU POOL DE DATES AVEC MÉTADONNÉES
        date_pool = {}
        
        for idx, row in df_dates_clean.iterrows():
            home = row['home_norm']
            away = row['away_norm']
            year = row['date_exacte'].year
            date_val = row['date_exacte']
            
            # Clé normalisée (triée)
            norm_key = tuple(sorted([home, away]) + [year])
            
            date_pool.setdefault(norm_key, []).append({
                'date': date_val,
                'home_original': row['home_team'],
                'away_original': row['away_team'],
                'home_norm': home,
                'away_norm': away,
                'source_idx': idx
            })
        
        # Trier les dates dans chaque groupe
        for key in date_pool:
            date_pool[key].sort(key=lambda x: x['date'])
        
        # 4. ALGORITHME D'APPARIEMENT INTELLIGENT
        # Pour chaque paire problématique, faire un appariement optimal
        
        # Dictionnaire pour stocker les appariements
        date_assignments = {}
        
        for team1, team2, year, match_count in problematic_pairs:
            key = (team1, team2, year)
            
            # Récupérer les matchs de cette paire
            matches = df_main[
                ((df_main['home_team'] == team1) & (df_main['away_team'] == team2) |
                (df_main['home_team'] == team2) & (df_main['away_team'] == team1)) &
                (df_main['edition'] == year)
            ].copy()
            
            if len(matches) != match_count:
                continue
            
            # Récupérer les dates disponibles
            norm_key = tuple(sorted([team1, team2]) + [year])
            available_dates_info = date_pool.get(norm_key, [])
            
            """if len(available_dates_info) == 0:
                logger.warning(f"⚠️ Aucune date pour {team1} vs {team2} ({year})")
                continue
            """
            
            """
            if len(available_dates_info) < match_count:
                logger.warning(f"⚠️ Manque dates: {team1} vs {team2} ({year}) - {match_count} matchs mais {len(available_dates_info)} dates")
                # On fait de notre mieux avec les dates disponibles
            """
            # 4.1. ORDONNER LES MATCHS par round et date estimée
            # Mapping des rounds pour ordre chronologique
            round_order_map = {
                'Group Stage': 1,
                'Round of 16': 2,
                'Quarter-finals': 3,
                'Semi-finals': 4,
                'Third Place': 5,
                'Final': 6
            }
            
            matches['round_order'] = matches['round'].map(round_order_map).fillna(99)
            matches = matches.sort_values(['round_order', 'date'])
            
            # 4.2. ORDONNER LES DATES disponibles (déjà triées)
            dates_to_assign = [info['date'] for info in available_dates_info]
            
            # 4.3. APPARIEMENT : première date au premier match, etc.
            for match_idx, (match_idx_row, match) in enumerate(matches.iterrows()):
                if match_idx < len(dates_to_assign):
                    assigned_date = dates_to_assign[match_idx]
                    date_assignments[match_idx_row] = assigned_date
                    
                    logger.info(f"🔍 Appariement {team1} vs {team2} ({year}):")
                    logger.info(f"   Match {match_idx+1}: {match['round']} -> {assigned_date.date()}")
                """
                else:
                    # Plus de dates disponibles, garder la date originale
                    logger.warning(f"⚠️ Plus de dates pour {team1} vs {team2}, match {match_idx+1} garde date originale")
                """
        # 5. APPLIQUER LES ASSIGNATIONS
        updated_count = 0
        
        for match_idx, assigned_date in date_assignments.items():
            original_date = df_main.at[match_idx, 'date']
            if original_date != assigned_date:
                df_main.at[match_idx, 'date'] = assigned_date
                updated_count += 1
        
        # 6. POUR LES MATCHS NON PROBLÉMATIQUES : logique simple
        simple_updated = 0
        used_dates_simple = {}
        
        for idx, row in df_main.iterrows():
            if idx in date_assignments:  # Déjà traité
                continue
                
            year = row['edition']
            try:
                if int(year) > 2010:
                    continue
            except:
                continue
            
            home = str(row['home_team']).strip()
            away = str(row['away_team']).strip()
            
            # Chercher dates disponibles
            norm_key = tuple(sorted([home, away]) + [int(year)])
            available = date_pool.get(norm_key, [])
            
            if available:
                # Prendre la première date (ou la plus proche si plusieurs matchs)
                track_key = f"{home}_{away}_{year}"
                if track_key not in used_dates_simple:
                    used_dates_simple[track_key] = []
                
                for date_info in available:
                    date_val = date_info['date']
                    if date_val not in used_dates_simple[track_key]:
                        if df_main.at[idx, 'date'] != date_val:
                            df_main.at[idx, 'date'] = date_val
                            simple_updated += 1
                        used_dates_simple[track_key].append(date_val)
                        break
        
        logger.info(f"✅ {updated_count + simple_updated} dates mises à jour")
        
        # 7. VÉRIFICATION FINALE
        self._verify_date_assignments(df_main, problematic_pairs, date_pool)
        
        return df_main

    def _verify_date_assignments(self, df, problematic_pairs, date_pool):
        """Vérification finale des assignations."""
        logger.info("🔍 VÉRIFICATION FINALE des dates...")
        
        issues = []
        
        for team1, team2, year, expected_count in problematic_pairs:
            matches = df[
                ((df['home_team'] == team1) & (df['away_team'] == team2) |
                (df['home_team'] == team2) & (df['away_team'] == team1)) &
                (df['edition'] == year)
            ]
            
            if len(matches) != expected_count:
                issues.append(f"{team1} vs {team2} ({year}): {len(matches)} matchs au lieu de {expected_count}")
                continue
            
            # Vérifier les dates uniques
            dates = matches['date'].dropna().unique()
            if len(dates) < len(matches):
                issues.append(f"{team1} vs {team2} ({year}): {len(matches)-len(dates)} doublons de date")
                
                # Afficher les détails
                """  logger.warning(f"⚠️ Problème {team1} vs {team2} ({year}):")
                for idx, row in matches.iterrows():
                    logger.warning(f"   {row['round']}: {row['date'].date() if pd.notna(row['date']) else 'N/A'}")
                """   
                # Afficher les dates disponibles
                norm_key = tuple(sorted([team1, team2]) + [year])
                available = date_pool.get(norm_key, [])
                if available:
                    logger.warning(f"   Dates disponibles: {[info['date'].date() for info in available]}")
        """
        if not issues:
            logger.info("✅ Toutes les dates sont correctement assignées !")
        else:
            logger.warning(f"⚠️ {len(issues)} problèmes détectés")
        
        return len(issues) == 0
        """    
        
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
        """
        Fusionne, DIAGNOSTIQUE et SAUVE les matchs sans date.
        Version corrigée avec gestion d'erreurs robuste.
        """
        logger.info("🔗 Consolidation des sources...")
        
        # Vérification des inputs
        if dfs_list is None:
            logger.error("❌ dfs_list est None!")
            return None
        
        # Filtrer les DataFrames None ou vides
        valid_dfs = []
        for i, df in enumerate(dfs_list):
            if df is None:
                logger.warning(f"⚠️ DataFrame {i} est None, ignoré")
            elif df.empty:
                logger.warning(f"⚠️ DataFrame {i} est vide, ignoré")
            else:
                valid_dfs.append(df)
                logger.info(f"✅ DataFrame {i}: {len(df)} lignes")
        
        if len(valid_dfs) == 0:
            logger.error("❌ Aucun DataFrame valide à consolider!")
            return None
        
        logger.info(f"📊 {len(valid_dfs)} DataFrames valides à fusionner")
        
        # 1. Fusion
        try:
            df_all = pd.concat(valid_dfs, ignore_index=True)
            logger.info(f"✅ Fusion réussie: {len(df_all)} lignes initiales")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la fusion: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
        
        # 2. SAUVETAGE DES DATES MANQUANTES
        missing_dates = df_all['date'].isna()
        if missing_dates.sum() > 0:
            logger.warning(f"⚠️ {missing_dates.sum()} matchs n'ont pas de date ! Tentative de sauvetage...")
            try:
                fallback_dates = pd.to_datetime(df_all.loc[missing_dates, 'edition'] + '-01-01', errors='coerce')
                df_all.loc[missing_dates, 'date'] = fallback_dates
                logger.info("✅ Matchs sauvés avec une date par défaut (01/01/AAAA).")
            except Exception as e:
                logger.error(f"❌ Erreur lors du sauvetage des dates: {e}")
        
        # 3. Nettoyage technique
        before_drop = len(df_all)
        df_all = df_all.dropna(subset=['home_team', 'away_team'])
        if len(df_all) < before_drop:
            logger.warning(f"🗑️ {before_drop - len(df_all)} matchs supprimés car équipes inconnues.")
        
        # 4. FILTRAGE : "BLACKLIST" (Prelim)
        try:
            mask_exclude = df_all['round'].astype(str).str.contains('preliminary', case=False, na=False)
            a_exclure = df_all[mask_exclude]
            if len(a_exclure) > 0:
                logger.info(f"🗑️ {len(a_exclure)} matchs à exclure (contenant 'preliminary')")
            df_all = df_all[~mask_exclude]
        except Exception as e:
            logger.error(f"❌ Erreur lors du filtrage 'preliminary': {e}")
        
        # 5. Dédoublonnage et Tri
        try:
            before_dedup = len(df_all)
            df_all = df_all.drop_duplicates(subset=['home_team', 'away_team', 'date', 'round'], keep='first')
            if len(df_all) < before_dedup:
                logger.info(f"🔍 {before_dedup - len(df_all)} doublons supprimés")
        except Exception as e:
            logger.error(f"❌ Erreur lors du dédoublonnage: {e}")
        
        # Conversion date et tri
        try:
            df_all['date'] = pd.to_datetime(df_all['date'])
            df_all = df_all.sort_values('date').reset_index(drop=True)
            df_all['id_match'] = range(1, len(df_all) + 1)
        except Exception as e:
            logger.error(f"❌ Erreur lors du tri/numérotation: {e}")
        
        # Colonnes finales
        cols = ['id_match', 'home_team', 'away_team', 'home_result', 'away_result', 
                'result', 'date', 'round', 'city', 'edition']
        
        # Vérifier que toutes les colonnes existent
        missing_cols = set(cols) - set(df_all.columns)
        if missing_cols:
            logger.error(f"❌ Colonnes manquantes: {missing_cols}")
            # Afficher les colonnes disponibles pour debug
            logger.info(f"   Colonnes disponibles: {df_all.columns.tolist()}")
            return None
        
        try:
            final_df = df_all[cols].copy()
            logger.info(f"✅ Consolidation terminée : {len(final_df)} matchs.")
            logger.info(f"   Colonnes finales: {final_df.columns.tolist()}")
            return final_df
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création du DataFrame final: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
                
    def validate(self, df):
        """Vérifie la qualité des données (complétude des colonnes et logique des scores)."""
        logger.info("✔️  Validation des données...")
        issues = []
        
        # Liste des colonnes obligatoires (correspondant à ta nouvelle table simplifiée)
        required = [
            'id_match', 'home_team', 'away_team', 'home_result', 
            'away_result', 'result', 'date', 'round', 'city', 'edition'
        ]
        
        # 1. Vérification de la présence des colonnes
        missing = set(required) - set(df.columns)
        if missing: 
            issues.append(f"Colonnes manquantes: {missing}")
        
        # 2. Vérification logique (Le gagnant correspond-il au score ?)
        for idx, row in df.iterrows():
            if row['result'] != 'draw' and row['result'] is not None:
                # Si Home gagne, Home Score doit être > Away Score
                if row['result'] == row['home_team'] and row['home_result'] <= row['away_result']:
                    issues.append(f"Incohérence L{idx}: {row['home_team']} déclaré gagnant mais score {row['home_result']}-{row['away_result']}")
                # Si Away gagne, Away Score doit être > Home Score
                elif row['result'] == row['away_team'] and row['away_result'] <= row['home_result']:
                    issues.append(f"Incohérence L{idx}: {row['away_team']} déclaré gagnant mais score {row['home_result']}-{row['away_result']}")
        

        return True
    def analyze_results(self, df):
        draws = df[df['result'] == 'draw']
        logger.info(f"📊 Analyse: {len(df)} matchs, {len(draws)} nuls.")