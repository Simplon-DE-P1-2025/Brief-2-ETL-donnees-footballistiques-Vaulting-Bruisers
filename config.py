import logging

# =====================================================================
# MODULE DE CONFIGURATION & REFERENTIELS
# Ce fichier centralise toutes les constantes statiques et paramètres
# globaux du projet ETL pour garantir la cohérence des traitements.
# =====================================================================

def setup_logging():
    """
    Initialise la configuration du logging pour l'ensemble de l'application.
    Format standardisé : Timestamp - Niveau - Message.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# =====================================================================
# DICTIONNAIRES DE MAPPING (REFERENTIELS METIER)
# Utilisés lors de l'étape de TRANSFORMATION pour normaliser les données.
# =====================================================================

TEAMS_MAPPING = {
    # --- Normalisation Historique & Géopolitique ---
    # Fusion RFA/RDA -> Allemagne
    "West Germany": "Germany",
    "FR Germany": "Germany",
    "German DR": "East Germany",
    "Germany FR": "Germany",
    "FRG": "Germany",  
    "Frg": "Germany", 

    # URSS -> Russie (successeur légal FIFA)
    "Soviet Union": "Russia",
    "USSR": "Russia",

    # Ex-Yougoslavie & Tchécoslovaquie
    "Yugoslavia": "Serbia",
    "Czechoslovakia": "Czech Republic",

    # --- Variations de Nommage ---
    "Korea Republic": "South Korea",
    "Korea DPR": "North Korea",
    "South Korea": "South Korea",
    
    "United States": "USA",
    "US": "USA",
    "Usa": "USA",            

    "Ivory Coast": "Cote d'Ivoire",
    "Côte d'Ivoire": "Cote d'Ivoire",
    
    "Bosnia-Herzegovina": "Bosnia and Herzegovina",
    
    # Crucial pour la jointure 2022 (IR Iran vs Iran)
    "Iran": "Iran",
    "IR Iran": "Iran",
    
    "Irish Republic": "Republic of Ireland",
    "Northern Ireland": "Northern Ireland",
    
    # --- Normalisation de Casse ---
    "FRANCE": "France",
    "BRAZIL": "Brazil",
    "ARGENTINA": "Argentina",
}

CITIES_MAPPING = {
    # --- Harmonisation des Villes Hôtes ---
    # Standardisation (ex: Mexico City vs México)
    "MONTEVIDEO": "Montevideo",

    "MEXICO CITY": "Mexico City",
    "Mexico (México)": "Mexico City",
    "México": "Mexico City",
    
    "Sao Paulo": "São Paulo",
    "São Paulo": "São Paulo",
    
    "Rio de Janeiro": "Rio de Janeiro",
    
    # St Denis est techniquement une banlieue, rattaché à Paris pour simplification analytique
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
    # --- Standardisation des Phases de Tournoi ---
    # Regroupement de toutes les phases de poules
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
    
    # Phases à élimination directe
    "8e de finale": "Round of 16",
    "Round of 16": "Round of 16",
    "ROUND_OF_16": "Round of 16",
    "Eighth-finals": "Round of 16",
    
    "1/4 finale": "Quarter-finals",
    "Quarter-finals": "Quarter-finals",
    "QUARTER_FINALS": "Quarter-finals",
    "Quarterfinals": "Quarter-finals",
    
    "1/2 finale": "Semi-finals",
    "Semi-finals": "Semi-finals",
    "SEMI_FINALS": "Semi-finals",
    "Semifinals": "Semi-finals",
    
    "3rd place": "Third Place",
    "Match pour la 3e place": "Third Place",
    "Third place": "Third Place",
    "Play-off for third place": "Third Place",
    
    "Final": "Final",
    "Finale": "Final",
    "FINAL": "Final",
}

# --- Mapping ID spécifique JSON 2018 ---
# Correspondance ID technique -> Nom équipe
TEAMS_MAPPING_2018 = {
    1: "Russia", 2: "Saudi Arabia", 3: "Egypt", 4: "Uruguay",
    5: "Portugal", 6: "Spain", 7: "Morocco", 8: "Iran",
    9: "France", 10: "Australia", 11: "Peru", 12: "Denmark",
    13: "Argentina", 14: "Iceland", 15: "Croatia", 16: "Nigeria",
    17: "Brazil", 18: "Switzerland", 19: "Costa Rica", 20: "Serbia",
    21: "Germany", 22: "Mexico", 23: "Sweden", 24: "South Korea",
    25: "Belgium", 26: "Panama", 27: "Tunisia", 28: "England",
    29: "Poland", 30: "Senegal", 31: "Colombia", 32: "Japan"
}

# Correspondance ID technique -> Nom stade
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