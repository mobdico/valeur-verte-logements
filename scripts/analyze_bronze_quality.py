#!/usr/bin/env python3
"""
Script d'analyse de la qualité des données BRONZE
Vérifie les NA, valeurs manquantes, et format des données
"""

import boto3
import json
import pandas as pd
import logging
from pathlib import Path
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bronze_quality.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BronzeQualityAnalyzer:
    def __init__(self, bucket_name="datalake-bronze"):
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )
        self.bucket_name = bucket_name

    def list_bronze_files(self):
        """Liste tous les fichiers dans la couche bronze"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=""
            )
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append(obj['Key'])
            logger.info(f"Trouvés {len(files)} fichiers dans {self.bucket_name}")
            return files
        except Exception as e:
            logger.error(f"Erreur lors de la liste des fichiers: {e}")
            return []

    def analyze_dpe_quality(self):
        """Analyse la qualité des données DPE"""
        logger.info("=== ANALYSE QUALITÉ DPE ===")
        
        # Lister les fichiers DPE
        dpe_files = [f for f in self.list_bronze_files() if f.startswith('dpe/')]
        
        if not dpe_files:
            logger.warning("Aucun fichier DPE trouvé")
            return
        
        # Analyser quelques fichiers pour échantillon
        sample_files = dpe_files[:3]  # Premier 3 fichiers
        
        for file_key in sample_files:
            logger.info(f"Analyse de {file_key}")
            try:
                # Télécharger et analyser
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
                data = json.loads(response['Body'].read())
                
                if isinstance(data, list) and len(data) > 0:
                    # Analyser le premier enregistrement
                    sample_record = data[0]
                    logger.info(f"  Champs disponibles: {list(sample_record.keys())}")
                    
                    # Compter les NA par champ
                    na_counts = {}
                    for field, value in sample_record.items():
                        if value is None or value == "" or value == "NA":
                            na_counts[field] = 1
                        else:
                            na_counts[field] = 0
                    
                    logger.info(f"  NA dans l'échantillon: {na_counts}")
                    
                    # Analyser tous les enregistrements
                    total_records = len(data)
                    field_na_counts = {field: 0 for field in sample_record.keys()}
                    
                    for record in data:
                        for field, value in record.items():
                            if value is None or value == "" or value == "NA":
                                field_na_counts[field] += 1
                    
                    logger.info(f"  Total enregistrements: {total_records}")
                    logger.info(f"  NA par champ: {field_na_counts}")
                    
                    # Pourcentages
                    for field, na_count in field_na_counts.items():
                        percentage = (na_count / total_records) * 100
                        logger.info(f"    {field}: {na_count}/{total_records} ({percentage:.1f}%)")
                        
            except Exception as e:
                logger.error(f"Erreur lors de l'analyse de {file_key}: {e}")

    def analyze_dvf_quality(self):
        """Analyse la qualité des données DVF"""
        logger.info("=== ANALYSE QUALITÉ DVF ===")
        
        # Lister les fichiers DVF
        dvf_files = [f for f in self.list_bronze_files() if f.startswith('dvf/')]
        
        if not dvf_files:
            logger.warning("Aucun fichier DVF trouvé")
            return
        
        # Analyser quelques fichiers
        sample_files = dvf_files[:2]  # Premier 2 fichiers
        
        for file_key in sample_files:
            logger.info(f"Analyse de {file_key}")
            try:
                # Télécharger et analyser
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
                content = response['Body'].read().decode('utf-8')
                
                # Analyser les premières lignes
                lines = content.split('\n')[:10]  # Premières 10 lignes
                logger.info(f"  Premières lignes analysées: {len(lines)}")
                
                # Analyser la structure
                for i, line in enumerate(lines):
                    if line.strip():
                        fields = line.split('|')  # DVF utilise | comme séparateur
                        logger.info(f"  Ligne {i+1}: {len(fields)} champs")
                        if i == 0:  # Première ligne
                            logger.info(f"    Champs: {fields}")
                        
                        # Compter les champs vides
                        empty_fields = sum(1 for field in fields if not field.strip())
                        logger.info(f"    Champs vides: {empty_fields}/{len(fields)}")
                        
            except Exception as e:
                logger.error(f"Erreur lors de l'analyse de {file_key}: {e}")

    def generate_quality_report(self):
        """Génère un rapport complet de qualité"""
        logger.info("=== RAPPORT DE QUALITÉ BRONZE ===")
        
        # Statistiques générales
        all_files = self.list_bronze_files()
        dpe_files = [f for f in all_files if f.startswith('dpe/')]
        dvf_files = [f for f in all_files if f.startswith('dvf/')]
        
        logger.info(f"📊 STATISTIQUES GÉNÉRALES:")
        logger.info(f"  Total fichiers: {len(all_files)}")
        logger.info(f"  Fichiers DPE: {len(dpe_files)}")
        logger.info(f"  Fichiers DVF: {len(dvf_files)}")
        
        # Analyser chaque type
        self.analyze_dpe_quality()
        self.analyze_dvf_quality()
        
        logger.info("=== RAPPORT TERMINÉ ===")

def main():
    logger.info("=== ANALYSE QUALITÉ DONNÉES BRONZE ===")
    analyzer = BronzeQualityAnalyzer()
    analyzer.generate_quality_report()

if __name__ == "__main__":
    main()
