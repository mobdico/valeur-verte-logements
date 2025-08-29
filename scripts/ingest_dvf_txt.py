#!/usr/bin/env python3
"""
Script d'ingestion des données DVF depuis les fichiers TXT/CSV
Ingestion BRONZE - Format natif (brut) sans typage agressif
"""

import os
os.makedirs("logs", exist_ok=True)

import logging
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dvf_txt.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DVFIngestion:
    def __init__(self,
                 source_dir="data/raw/dvf",
                 bucket_name="datalake-bronze"):

        self.source_dir = Path(source_dir)
        self.bucket_name = bucket_name

        # MinIO (dans Docker)
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )

        # Création du bucket si besoin
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} existe déjà")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket"):
                logger.info(f"Création du bucket {self.bucket_name}")
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                raise

    def find_dvf_files(self):
        """Trouve tous les fichiers DVF brut dans le dossier source"""
        patterns = ["*.txt", "*.TXT", "*.csv", "*.CSV"]
        files = []
        for pattern in patterns:
            files.extend(self.source_dir.glob(pattern))
        logger.info(f"Trouvés {len(files)} fichiers DVF dans {self.source_dir}")
        return files

    def determine_year_from_filename(self, path: Path) -> str:
        """Détermine l'année à partir du nom du fichier (fallback: mtime)"""
        name = path.name.lower()
        for y in ("2020", "2021", "2022", "2023"):
            if y in name:
                return y
        mod_time = datetime.fromtimestamp(path.stat().st_mtime)
        return str(mod_time.year)

    def upload_to_minio(self, source_file: Path, year: str):
        """Upload du fichier vers MinIO (BRONZE/dvf/<année>/...)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key = f"dvf/{year}/dvf_{year}_{timestamp}_{source_file.name}"

        try:
            self.s3_client.upload_file(str(source_file), self.bucket_name, key)
            logger.info(f"Upload OK: {source_file.name} -> s3://{self.bucket_name}/{key}")
            return key
        except ClientError as e:
            logger.error(f"Erreur upload {source_file.name}: {e}")
            return None

    def process_file(self, file_path: Path) -> bool:
        """Traite un fichier DVF brut"""
        logger.info(f"Traitement: {file_path.name}")
        year = self.determine_year_from_filename(file_path)
        logger.info(f"Année détectée: {year}")

        key = self.upload_to_minio(file_path, year)
        if key:
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"OK: {file_path.name} ({size_mb:.2f} MB) -> {key}")
            return True
        else:
            logger.error(f"Échec: {file_path.name}")
            return False

    def ingest_all(self):
        """Ingère tous les fichiers DVF présents"""
        logger.info("=== INGESTION DVF TXT ===")
        files = self.find_dvf_files()
        if not files:
            logger.warning(f"Aucun fichier dans {self.source_dir}")
            return 0, 0

        ok, ko = 0, 0
        for path in files:
            if self.process_file(path):
                ok += 1
            else:
                ko += 1

        logger.info("=== INGESTION DVF TERMINÉE ===")
        logger.info(f"Succès: {ok} | Échecs: {ko} | Total: {len(files)}")
        return ok, ko

def main():
    logger.info("=== Lancement ingestion DVF ===")
    dvf = DVFIngestion()
    ok, ko = dvf.ingest_all()
    if ko == 0:
        logger.info("✅ Tous les fichiers DVF ont été traités avec succès!")
    else:
        logger.warning(f"⚠️ {ko} fichiers en échec")

if __name__ == "__main__":
    main()
