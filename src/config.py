"""
Configuration centralisée du projet Valeur Verte des Logements
"""

import os
from pathlib import Path
from typing import Dict, Any

# Chemins du projet
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

# Configuration MinIO
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "admin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "password123"),
    "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
    "region": "us-east-1"
}

# Configuration des buckets
BUCKETS = {
    "bronze": os.getenv("BRONZE_BUCKET", "datalake-bronze"),
    "silver": os.getenv("SILVER_BUCKET", "datalake-silver"),
    "gold": os.getenv("GOLD_BUCKET", "datalake-gold")
}

# Configuration des données
DATA_CONFIG = {
    "dpe": {
        "date_range": {
            "start": "2020-01-01",
            "end": "2021-06-30"
        },
        "fields": [
            "classe_consommation_energie",
            "classe_estimation_ges", 
            "date_etablissement_dpe",
            "surface_thermique_lot",
            "code_insee_commune_actualise",
            "tv016_departement_code"
        ]
    },
    "dvf": {
        "years": [2020, 2021],
        "fields": [
            "date_mutation",
            "valeur_fonciere",
            "surface_reelle_bati",
            "type_local",
            "code_commune",
            "code_departement"
        ]
    }
}

# Configuration des partitions
PARTITION_CONFIG = {
    "date_format": "%Y-%m-%d",
    "partition_columns": ["annee", "trimestre"],
    "file_format": "parquet"
}

# Configuration du logging
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": LOGS_DIR / "app.log"
}

def get_config() -> Dict[str, Any]:
    """Retourne la configuration complète du projet"""
    return {
        "project_root": str(PROJECT_ROOT),
        "data_dir": str(DATA_DIR),
        "minio": MINIO_CONFIG,
        "buckets": BUCKETS,
        "data_config": DATA_CONFIG,
        "partition_config": PARTITION_CONFIG,
        "logging": LOGGING_CONFIG
    }

def ensure_directories():
    """Crée les répertoires nécessaires s'ils n'existent pas"""
    directories = [DATA_DIR, LOGS_DIR, SCRIPTS_DIR, NOTEBOOKS_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Créer les sous-répertoires de données
    (DATA_DIR / "raw").mkdir(exist_ok=True)
    (DATA_DIR / "bronze").mkdir(exist_ok=True)
    (DATA_DIR / "silver").mkdir(exist_ok=True)
    (DATA_DIR / "gold").mkdir(exist_ok=True)

if __name__ == "__main__":
    # Test de la configuration
    print("🔧 Configuration du projet Valeur Verte des Logements")
    print("=" * 50)
    
    config = get_config()
    for key, value in config.items():
        print(f"{key}: {value}")
    
    print("\n📁 Création des répertoires...")
    ensure_directories()
    print("✅ Configuration terminée !")
