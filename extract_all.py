import json
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class WorldCupExtractor:
    """
    Module d'Extraction (Ingestion) de l'ETL.
    Responsable de la lecture des données brutes depuis diverses sources (CSV, JSON).
    Gère les spécificités techniques des fichiers (encodage, séparateurs, nettoyage bas niveau).
    """
    
    def __init__(self, data_dir="data/raw"):
        # Utilisation de pathlib pour une gestion des chemins compatible tous OS (Windows/Linux/Mac)
        self.data_dir = Path(data_dir)
        
    def extract_source1(self, filename="matches_1930-2010.csv"):
        """
        Ingestion du dataset historique (1930-2010).
        Format attendu : CSV standard.
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            df = pd.read_csv(filepath)
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise
    
    def extract_source2(self, filename="WorldCupMatches2014.csv"):
        """
        Ingestion du dataset spécifique 2014.
        Particularité : Fichier bruité nécessitant un nettoyage bas niveau des chaînes de caractères
        et une gestion robuste de l'encodage (tentative UTF-8-SIG puis Latin-1).
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            
            # Stratégie de fallback pour l'encodage (gestion des BOM et caractères spéciaux)
            try:
                df = pd.read_csv(filepath, sep=';', encoding='utf-8-sig')
            except:
                logger.warning("⚠️ Encodage UTF-8 échoué, tentative Latin-1")
                df = pd.read_csv(filepath, sep=';', encoding='latin-1')
            
            # Fonction locale de nettoyage des artefacts (ex: "rn"">) présents dans ce fichier spécifique
            def clean_cell(cell):
                if isinstance(cell, str):
                    cell = cell.replace('"rn"">', '').replace('"rn">', '')
                    cell = cell.replace('""', '"').strip('"')
                return cell
            
            # Application du nettoyage sur toutes les colonnes textuelles
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(clean_cell)
            
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise
    
    def extract_source3(self, filename="Fifa_world_cup_matches.csv"):
        """
        Ingestion du dataset 2022.
        Gère automatiquement la détection du séparateur (virgule ou point-virgule)
        et nettoie les espaces superflus dans les noms de colonnes.
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            # Tentative de lecture standard puis fallback sur séparateur ';'
            try:
                df = pd.read_csv(filepath, encoding='utf-8')
            except Exception:
                df = pd.read_csv(filepath, sep=';', encoding='utf-8')
            
            # Normalisation des en-têtes (suppression espaces début/fin)
            df.columns = [col.strip() for col in df.columns]
            logger.info(f"✅ {len(df)} matchs extraits de {filename}")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise

    def extract_source4(self, filename="data_2018.json"):
        """
        Ingestion du dataset 2018 au format JSON.
        Charge la structure hiérarchique complète (matchs, stades, équipes) en mémoire.
        """
        logger.info(f"📥 Extraction de {filename}...")
        try:
            filepath = self.data_dir / filename
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"✅ JSON 2018 chargé avec succès")
            return data
        except Exception as e:
            logger.error(f"❌ Erreur extraction {filename}: {e}")
            raise
    

    def extract_historical_dates(self, filename="dates_1930_2010.txt"):
        """
        Extraction optimisée pour le format TXT (virgules, dates ISO).
        """
        logger.info(f"📥 Extraction des dates historiques ({filename})...")
        try:
            filepath = self.data_dir / filename
            
            # Lecture standard CSV (virgule) avec gestion des guillemets
            # On utilise engine='python' pour une meilleure tolérance aux guillemets mal fermés
            try:
                df = pd.read_csv(filepath, sep=',', header=0, names=['home_team', 'away_team', 'date_exacte'], encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, sep=',', header=0, names=['home_team', 'away_team', 'date_exacte'], encoding='latin-1')

            # Nettoyage des lignes vides
            df = df.dropna(subset=['home_team', 'away_team', 'date_exacte'])

            # Fonction de nettoyage spécifique aux artefacts de ce fichier
            def clean_raw_text(x):
                if isinstance(x, str):
                    # Enlève "rn"">, rn"> et les guillemets superflus
                    x = x.replace('"rn"">', '').replace('rn">', '').replace('"', '')
                    return x.strip()
                return str(x)

            df['home_team'] = df['home_team'].apply(clean_raw_text)
            df['away_team'] = df['away_team'].apply(clean_raw_text)
            
            logger.info(f"✅ {len(df)} dates historiques chargées (Format TXT).")
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ Impossible de charger {filename}: {e}")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Impossible de charger {filename}: {e}")
            return None
    def extract_cities_2022(self, filename="cities_2022.csv"):
        """
        Ingestion du fichier de correction (Mapping Villes 2022).
        Utilisé pour enrichir les données manquantes de la source principale 2022.
        """
        logger.info(f"📥 Extraction des corrections villes 2022 ({filename})...")
        try:
            filepath = self.data_dir / filename
            # Lecture avec séparateur ';' (défaut pour ce fichier)
            df = pd.read_csv(filepath, sep=';')
            
            # Nettoyage des noms de colonnes pour garantir les jointures
            df.columns = [c.strip() for c in df.columns]
            
            logger.info(f"✅ {len(df)} lignes de correction chargées")
            return df
        except Exception as e:
            logger.warning(f"⚠️ Impossible de charger {filename}: {e}")
            return None