#!/usr/bin/env python3
"""
Script d'analyse de la qualité des données SILVER
Vérifie les NA résiduels et la qualité des données transformées
"""

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/silver_quality.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SilverQualityAnalyzer:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )
        self.silver_bucket = "datalake-silver"
        
        # PyArrow filesystem pour MinIO
        self.pa_fs = pafs.S3FileSystem(
            endpoint_override='http://minio:9000',
            access_key='admin',
            secret_key='password123',
            scheme='http'
        )

    def analyze_silver_dvf(self):
        """Analyse la qualité des données DVF SILVER"""
        logger.info("=== ANALYSE QUALITÉ DVF SILVER ===")
        
        try:
            # Lire un échantillon DVF SILVER
            dataset = pq.ParquetDataset(
                f"{self.silver_bucket}/dvf",
                filesystem=self.pa_fs
            )
            
            # Lire quelques partitions pour échantillon
            table = dataset.read()
            df = table.to_pandas()
            
            logger.info(f"DVF SILVER: {len(df)} lignes, {len(df.columns)} colonnes")
            logger.info(f"Colonnes: {list(df.columns)}")
            
            # Analyser les NA par colonne
            na_counts = df.isna().sum()
            na_percentages = (na_counts / len(df)) * 100
            
            logger.info("�� ANALYSE DES NA DVF SILVER:")
            for col in df.columns:
                na_count = na_counts[col]
                na_pct = na_percentages[col]
                logger.info(f"  {col}: {na_count}/{len(df)} ({na_pct:.1f}%)")
            
            # Statistiques des données
            logger.info(f"\nSTATISTIQUES DVF SILVER:")
            logger.info(f"  Valeur foncière: min={df['Valeur fonciere'].min():.2f}, max={df['Valeur fonciere'].max():.2f}")
            logger.info(f"  Surface bâtie: min={df['Surface reelle bati'].min():.2f}, max={df['Surface reelle bati'].max():.2f}")
            logger.info(f"  Prix au m²: min={df['prix_m2'].min():.2f}, max={df['prix_m2'].max():.2f}")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse DVF SILVER: {e}")

    def analyze_silver_dpe(self):
        """Analyse la qualité des données DPE SILVER"""
        logger.info("\n🔍 ANALYSE QUALITÉ DPE SILVER")
        logger.info("=" * 50)
        
        try:
            # Lecture DPE Silver
            dpe_dataset = pq.ParquetDataset(f"{self.silver_bucket}/dpe/", filesystem=self.pa_fs)
            dpe_table = dpe_dataset.read()
            dpe_df = dpe_table.to_pandas()
            
            logger.info(f"📊 DPE SILVER: {len(dpe_df):,} lignes")
            
            # Vérification NAs
            na_counts = dpe_df.isna().sum()
            na_percentages = (na_counts / len(dpe_df) * 100).round(2)
            
            logger.info("\n📋 VÉRIFICATION VALEURS MANQUANTES:")
            for col in dpe_df.columns:
                if na_counts[col] > 0:
                    logger.info(f"  {col}: {na_counts[col]:,} ({na_percentages[col]}%)")
                else:
                    logger.info(f"  {col}: 0 (0%)")
            
            # Distribution des classes DPE
            logger.info("\n DISTRIBUTION CLASSES DPE:")
            dpe_distribution = dpe_df['classe_consommation_energie'].value_counts().sort_index()
            for classe, count in dpe_distribution.items():
                percentage = (count / len(dpe_df) * 100).round(2)
                logger.info(f"  {classe}: {count:,} ({percentage}%)")
            
            # Statistiques de base
            logger.info(f"\nSTATISTIQUES:")
            logger.info(f"  Surface thermique min: {dpe_df['surface_thermique_lot'].min():.1f} m²")
            logger.info(f"  Surface thermique max: {dpe_df['surface_thermique_lot'].max():.1f} m²")
            logger.info(f"  Surface thermique moyenne: {dpe_df['surface_thermique_lot'].mean():.1f} m²")
            
            return dpe_df
            
        except Exception as e:
            logger.error(f"Erreur analyse DPE SILVER: {e}")
            return None

    def analyze_dpe_geographic_distribution(self):
        """Analyse la répartition géographique des classes DPE par département"""
        logger.info("\n ANALYSE RÉPARTITION GÉOGRAPHIQUE DPE")
        logger.info("=" * 60)
        
        try:
            # Lecture DPE Silver
            dpe_dataset = pq.ParquetDataset(f"{self.silver_bucket}/dpe/", filesystem=self.pa_fs)
            dpe_table = dpe_dataset.read()
            dpe_df = dpe_table.to_pandas()
            
            # Extraire le département du code INSEE
            dpe_df['departement'] = dpe_df['code_insee_commune_actualise'].astype(str).str[:2]
            
            # Filtrer nos départements d'intérêt
            deps_interet = ['92', '54', '34']
            dpe_filtered = dpe_df[dpe_df['departement'].isin(deps_interet)]
            
            logger.info(f"DPE dans nos départements: {len(dpe_filtered):,} lignes")
            
            # Analyse par département
            for dept in deps_interet:
                dept_data = dpe_filtered[dpe_filtered['departement'] == dept]
                if len(dept_data) > 0:
                    logger.info(f"\nDÉPARTEMENT {dept}:")
                    logger.info(f"  Total DPE: {len(dept_data):,}")
                    
                    # Distribution des classes par département
                    dept_dist = dept_data['classe_consommation_energie'].value_counts().sort_index()
                    for classe, count in dept_dist.items():
                        percentage = (count / len(dept_data) * 100).round(2)
                        logger.info(f"    {classe}: {count:,} ({percentage}%)")
                    
                    # Focus sur les passoires (F, G)
                    passoires = dept_data[dept_data['classe_consommation_energie'].isin(['F', 'G'])]
                    if len(passoires) > 0:
                        logger.info(f"    🏚️ Passoires (F+G): {len(passoires):,} ({(len(passoires)/len(dept_data)*100):.1f}%)")
                    else:
                        logger.info(f"    🏚️ Passoires (F+G): 0 (0%)")
                else:
                    logger.info(f"\n DÉPARTEMENT {dept}: Aucune donnée DPE")
            
            # Analyse globale des passoires
            passoires_global = dpe_filtered[dpe_filtered['classe_consommation_energie'].isin(['F', 'G'])]
            logger.info(f"\n ANALYSE GLOBALE PASSOIRES:")
            logger.info(f"  Total passoires (F+G): {len(passoires_global):,}")
            logger.info(f"  % passoires: {(len(passoires_global)/len(dpe_filtered)*100):.2f}%")
            
            return dpe_filtered
            
        except Exception as e:
            logger.error(f"Erreur analyse géographique DPE: {e}")
            return None

    def generate_quality_report(self):
        """Génère un rapport complet de qualité SILVER"""
        logger.info("=== RAPPORT DE QUALITÉ SILVER ===")
        
        # Analyser chaque type
        self.analyze_silver_dvf()
        self.analyze_silver_dpe()
        
        # Analyser la répartition géographique DPE
        self.analyze_dpe_geographic_distribution()
        
        logger.info("=== RAPPORT TERMINÉ ===")

def main():
    logger.info("=== ANALYSE QUALITÉ DONNÉES SILVER ===")
    analyzer = SilverQualityAnalyzer()
    analyzer.generate_quality_report()

if __name__ == "__main__":
    main()