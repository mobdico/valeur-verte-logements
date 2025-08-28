#!/usr/bin/env python3
"""
Script d'ingestion des données DPE depuis l'API ADEME data-fair
Ingestion BRONZE - Format natif JSON avec pagination
"""

import requests
import json
import os
import time
from datetime import datetime
from pathlib import Path
import logging
import boto3
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import quote

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dpe_api.log'),
        logging.StreamHandler()  # Garde l'affichage console
    ]
)
logger = logging.getLogger(__name__)

class DPEIngestion:
    def __init__(self, bucket_name="datalake-bronze"):
        # Configuration MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )
        
        # Créer le bucket s'il n'existe pas
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} existe déjà")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Création du bucket {bucket_name}")
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                raise e
        
        self.bucket_name = bucket_name
        
        # Paramètres de l'API (basés sur votre code fonctionnel)
        self.dataset = "dpe-france"
        self.base_url = f"https://data.ademe.fr/data-fair/api/v1/datasets/{self.dataset}/lines"
        self.departements = ["92", "59", "34"]  # Hauts-de-Seine, Nord, Hérault
        self.date_start = "2020-01-01"
        self.date_end = "2021-06-30"
        self.size = 10000  # max 10000 lignes/page
        
        # Champs à récupérer
        self.select_fields = [
            "numero_dpe",
            "date_etablissement_dpe",
            "code_insee_commune_actualise",
            "classe_consommation_energie",
            "classe_estimation_ges",
            "tr002_type_batiment_description",
            "tv016_departement_code",
        ]
        
    def build_url(self, dept: str, after=None):
        """Construit l'URL pour un département"""
        select = ",".join(self.select_fields)
        qs = f'tv016_departement_code:"{dept}" AND date_etablissement_dpe:[{self.date_start} TO {self.date_end}]'
        url = f"{self.base_url}?select={quote(select)}&qs={quote(qs)}&size={self.size}"
        
        if after:
            url += f"&after={after}"
        
        return url
    
    def new_session(self, timeout=60):
        """Crée une session avec retry et rate limiting"""
        s = requests.Session()
        retries = Retry(
            total=6, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        s.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=10))
        s.headers.update({"Accept": "application/json"})
        s.request_timeout = timeout
        return s
    
    def fetch_page(self, session: requests.Session, url: str):
        """Récupère une page de données avec rate limiting"""
        time.sleep(0.13)  # ~7-8 req/s, sous 10 req/s max
        logger.info(f"→ Fetch: {url}")
        
        try:
            r = session.get(url, timeout=session.request_timeout)
            r.raise_for_status()
            j = r.json()
            return j.get("results", []), j.get("next")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la récupération: {e}")
            return [], None
    
    def save_batch_to_minio(self, data, dept, batch_num):
        """Sauvegarde un lot de données en JSON dans MinIO"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dpe/{dept}/dpe_batch_{batch_num:04d}_{timestamp}.json"
        
        try:
            # Sauvegarde temporaire locale
            temp_file = f"/tmp/dpe_batch_{batch_num:04d}_{timestamp}.json"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Upload vers MinIO
            self.s3_client.upload_file(temp_file, self.bucket_name, filename)
            
            # Nettoyage du fichier temporaire
            os.remove(temp_file)
            
            logger.info(f"Lot {batch_num} sauvegardé: s3://{self.bucket_name}/{filename}")
            return filename
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du lot {batch_num}: {e}")
            return None
    
    def ingest_dept(self, dept: str, max_batches=None):
        """Ingère toutes les données DPE pour un département"""
        logger.info(f"=== Département {dept} ===")
        
        url = self.build_url(dept)
        session = self.new_session()
        
        batch_num = 1
        total_records = 0
        
        while url and (not max_batches or batch_num <= max_batches):
            logger.info(f"Récupération du lot {batch_num}...")
            results, next_url = self.fetch_page(session, url)
            
            if results:
                # Sauvegarde du lot
                filename = self.save_batch_to_minio(results, dept, batch_num)
                if not filename:
                    logger.error(f"Échec de sauvegarde du lot {batch_num}")
                    break
                
                # Statistiques
                total_records += len(results)
                logger.info(f"Lot {batch_num}: {len(results)} enregistrements")
                
                # Pagination
                url = next_url
                if next_url:
                    logger.info(f"Token de pagination suivant: {next_url}")
                else:
                    logger.info("Aucun token de pagination - fin des données")
                    break
            else:
                logger.warning(f"Lot {batch_num} vide")
                break
                
            batch_num += 1
        
        logger.info(f"[{dept}] Terminé → {total_records} enregistrements en {batch_num-1} lots")
        return total_records, batch_num-1
    
    def ingest_all(self, max_batches_per_dept=None):
        """Ingère toutes les données DPE pour tous les départements"""
        logger.info("Début de l'ingestion DPE...")
        
        total_records = 0
        total_batches = 0
        
        for dept in self.departements:
            records, batches = self.ingest_dept(dept, max_batches_per_dept)
            total_records += records
            total_batches += batches
        
        logger.info(f"Ingestion terminée. Total: {total_records} enregistrements en {total_batches} lots")
        return total_records, total_batches

def main():
    """Fonction principale"""
    logger.info("=== INGESTION DPE API ===")
    
    # Création de l'instance d'ingestion
    dpe_ingestion = DPEIngestion()
    
    # Pour les tests, limitons à 2 lots par département
    # En production, supprimez max_batches_per_dept
    total_records, total_batches = dpe_ingestion.ingest_all(max_batches_per_dept=2)
    
    logger.info(f"=== INGESTION TERMINÉE ===")
    logger.info(f"Total enregistrements: {total_records}")
    logger.info(f"Total lots: {total_batches}")
    logger.info(f"Données sauvegardées dans MinIO: s3://{dpe_ingestion.bucket_name}/dpe/")

if __name__ == "__main__":
    main()
