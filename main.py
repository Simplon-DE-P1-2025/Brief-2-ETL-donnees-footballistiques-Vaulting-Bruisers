"""
ETL Pipeline - FIFA World Cup Matches (1930-2022)
Pipeline complet : Extract -> Transform -> Load

Ce script agit comme le point d'entrée (Orchestrateur) du projet.
Il coordonne les interactions entre les modules d'extraction, de transformation et de chargement.
"""
import logging
from pathlib import Path
from config import setup_logging
from extract import WorldCupExtractor
from transform import WorldCupTransformer
from load import WorldCupLoader

def run_etl_pipeline():
    """
    Fonction principale d'orchestration.
    Exécute séquentiellement les étapes E-T-L avec gestion des erreurs et logging centralisé.
    """
    # Initialisation du système de logs
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*70)
    logger.info("DÉBUT DU PIPELINE ETL - World Cup Data")
    logger.info("="*70)
    
    try:
        # =====================================================================
        # PHASE 1 : EXTRACTION (Ingestion)
        # Lecture des sources hétérogènes (CSV historiques, JSON, Fichiers correctifs)
        # =====================================================================
        extractor = WorldCupExtractor(data_dir="data/raw")
        
        # Chargement des datasets bruts en mémoire (DataFrames)
        df1 = extractor.extract_source1("matches_1930-2010.csv")
        df2 = extractor.extract_source2("WorldCupMatches2014.csv")
        df3 = extractor.extract_source3("Fifa_world_cup_matches.csv")
        json4 = extractor.extract_source4("data_2018.json")
        dates_1930_2010 = extractor.extract_historical_dates("dates_1930_2010.txt")
        
        # Chargement du fichier de référence pour la correction des villes 2022
        cities_22 = extractor.extract_cities_2022("cities_2022.csv")
        
        # =====================================================================
        # PHASE 2 : TRANSFORMATION
        # Nettoyage, Normalisation, Enrichissement et Consolidation
        # =====================================================================
        transformer = WorldCupTransformer()
        
        # Transformation unitaire de chaque source (Parsing dates, scores, noms)
        df_c1 = transformer.transform_source1(df1)
        if dates_1930_2010 is not None:
            df_c1 = transformer.enrich_with_historical_dates(df_c1, dates_1930_2010)
        df_c2 = transformer.transform_source2(df2)
        df_c3 = transformer.transform_source3(df3)
        df_c4 = transformer.transform_source4(json4)

        # Application des règles métier spécifiques (Enrichissement)
        # 1. Correction des villes 2022 via jointure externe
        if cities_22 is not None:
            df_c3 = transformer.enrich_2022_with_cities(df_c3, cities_22)
    
            
        # Fusion des datasets et génération de l'ID unique
        df_final = transformer.consolidate([df_c1, df_c2, df_c3, df_c4])
        
        
        # DEBUG CRITIQUE
        if df_final is None:
            logger.error("ERREUR CRITIQUE: consolidate() a retourné None!")
            logger.error("   Arrêt du pipeline car pas de données à charger.")
            # Vous pouvez soit arrêter ici, soit continuer avec un DataFrame vide
            df_final = pd.DataFrame()  # Créer un DataFrame vide pour éviter l'erreur suivante
        else:
            logger.info(f"consolidate() a retourné {len(df_final)} lignes")

        # Audit Qualité des Données (Data Quality)
        transformer.analyze_results(df_final)
        
        if not transformer.validate(df_final):
            logger.warning(" Problèmes de validation détectés (pipeline continue)")
        # =====================================================================
        # PHASE 3 : CHARGEMENT (Load)
        # Persistance des données nettoyées dans le Data Warehouse (SQLite)
        # =====================================================================
        loader = WorldCupLoader(db_path="data/worldcup.db")
        loader.connect()
        
        # Réinitialisation du schéma (DDL)
        loader.create_schema()
        
        # Insertion des données (DML)
        loader.load_data(df_final)             # Table de faits (Matchs)
        
        # Vérification finale post-chargement
        loader.verify_load()
        loader.close()
        
        # Export Flat File (CSV) pour audit ou usage BI léger
        df_final.to_csv("data/processed/worldcup_clean.csv", index=False)
        logger.info("=" * 70)
        logger.info("PIPELINE TERMINÉ AVEC SUCCÈS")
        logger.info("=" * 70)
                
    except Exception as e:
        # Gestion globale des exceptions critiques
        logger.error(f"ERREUR PIPELINE: {e}")
        import traceback
        traceback.print_exc()

# =====================================================================
# POINT D'ENTRÉE (MAIN)
# =====================================================================
if __name__ == "__main__":
    # Préparation de l'environnement d'exécution (arborescence fichiers)
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    
    # Lancement de l'orchestrateur
    run_etl_pipeline()