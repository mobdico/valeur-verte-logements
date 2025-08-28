#!/usr/bin/env python3
"""
Script d'ingestion des données DVF depuis les fichiers TXT
Ingestion BRONZE - Format natif TXT sans typage agressif
"""

import os
import shutil
import logging
from pathlib import Path
from datetime import datetime
import glob
import boto3
from botocore.exceptions import ClientError

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dvf_txt.log'),
        logging.StreamHandler()  # Garde l'affichage console
    ]
)
logger = logging.getLogger(__name__)

class DVFIngestion:
    def __init__(self, 
                 source_dir="data/raw/dvf",
                 bucket_name="datalake-bronze"):
        self.source_dir = Path(source_dir)
        self.bucket_name = bucket_name
        
        # Configuration MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',  # Nom du service Docker
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )
        
        # Créer le bucket s'il n'existe pas
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} existe déjà")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Création du bucket {self.bucket_name}")
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                raise e
    
    def find_dvf_files(self):
        """Trouve tous les fichiers DVF dans le dossier source"""
        # Recherche des fichiers TXT et CSV
        patterns = ["*.txt", "*.TXT", "*.csv", "*.CSV"]
        files = []
        
        for pattern in patterns:
            files.extend(self.source_dir.glob(pattern))
        
        logger.info(f"Trouvés {len(files)} fichiers DVF dans {self.source_dir}")
        return files
    
    def determine_year_from_filename(self, filename):
        """Détermine l'année à partir du nom de fichier"""
        filename_lower = filename.name.lower()  # ✅ .name pour obtenir le nom du fichier
        
        # Recherche d'années dans le nom de fichier
        if "2020" in filename_lower:
            return "2020"
        elif "2021" in filename_lower:
            return "2021"
        elif "2022" in filename_lower:
            return "2022"
        elif "2023" in filename_lower:
            return "2023"
        else:
            # Si pas d'année dans le nom, on utilise la date de modification
            stat = filename.stat()
            mod_time = datetime.fromtimestamp(stat.st_mtime)
            return str(mod_time.year)
    
    def upload_to_minio(self, source_file, year):
        """Uploade le fichier vers MinIO"""
        # Nom du fichier de destination avec année et timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_filename = f"dvf/{year}/dvf_{year}_{timestamp}_{source_file.name}"
        
        try:
            # Upload vers MinIO
            self.s3_client.upload_file(
                str(source_file),
                self.bucket_name,
                dest_filename
            )
            logger.info(f"Fichier uploadé: {source_file.name} -> s3://{self.bucket_name}/{dest_filename}")
            return dest_filename
        except ClientError as e:
            logger.error(f"Erreur lors de l'upload de {source_file.name}: {e}")
            return None
    
    def process_file(self, file_path):
        """Traite un fichier DVF individuel"""
        logger.info(f"Traitement du fichier: {file_path.name}")
        
        # Détermination de l'année
        year = self.determine_year_from_filename(file_path)
        logger.info(f"Année déterminée: {year}")
        
        # Upload vers MinIO
        dest_path = self.upload_to_minio(file_path, year)
        
        if dest_path:
            # Vérification de la taille du fichier source
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"Fichier traité avec succès: {file_path.name} ({size_mb:.2f} MB) -> {dest_path}")
            return True
        else:
            logger.error(f"Échec du traitement du fichier: {file_path.name}")
            return False
    
    def ingest_all(self):
        """Ingère tous les fichiers DVF trouvés"""
        logger.info("=== INGESTION DVF TXT ===")
        
        # Recherche des fichiers
        files = self.find_dvf_files()
        
        if not files:
            logger.warning(f"Aucun fichier DVF trouvé dans {self.source_dir}")
            return 0, 0
        
        # Traitement des fichiers
        successful = 0
        failed = 0
        
        for file_path in files:
            if self.process_file(file_path):
                successful += 1
            else:
                failed += 1
        
        logger.info(f"=== INGESTION DVF TERMINÉE ===")
        logger.info(f"Fichiers traités avec succès: {successful}")
        logger.info(f"Fichiers en échec: {failed}")
        logger.info(f"Total: {len(files)}")
        
        return successful, failed

def main():
    """Fonction principale"""
    logger.info("=== INGESTION DVF TXT ===")
    
    # Création de l'instance d'ingestion
    dvf_ingestion = DVFIngestion()
    
    # Traitement de tous les fichiers
    successful, failed = dvf_ingestion.ingest_all()
    
    if failed == 0:
        logger.info("✅ Tous les fichiers DVF ont été traités avec succès!")
    else:
        logger.warning(f"⚠️ {failed} fichiers n'ont pas pu être traités")

if __name__ == "__main__":
    main()
