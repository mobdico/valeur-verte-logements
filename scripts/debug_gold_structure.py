#!/usr/bin/env python3
"""
Script de debug pour examiner la structure des données GOLD
"""

import logging
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pafs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_gold_structure():
    """Debug la structure des données GOLD"""
    logger.info("🔍 DEBUG STRUCTURE DONNÉES GOLD")
    logger.info("=" * 50)
    
    try:
        # Connexion MinIO
        pa_fs = pafs.S3FileSystem(
            endpoint_override='http://minio:9000',
            access_key='admin',
            secret_key='password123',
            scheme='http'
        )
        
        # Charger les données GOLD
        logger.info("📖 Chargement des données GOLD...")
        gold_dataset = ds.dataset("datalake-gold/market_indicators", filesystem=pa_fs)
        
        # Vérifier le schéma
        logger.info("📋 SCHÉMA DU DATASET:")
        logger.info(f"  Schema: {gold_dataset.schema}")
        
        # Vérifier les partitions
        logger.info("📁 PARTITIONS:")
        logger.info(f"  Partitioning: {gold_dataset.partitioning}")
        
        # Charger un échantillon
        table = gold_dataset.to_table()
        df = table.to_pandas()
        
        logger.info(f"📊 Données: {len(df):,} lignes")
        logger.info(f"🏷️ Colonnes: {list(df.columns)}")
        
        # Afficher les premières lignes
        logger.info("\n📋 PREMIÈRES LIGNES:")
        logger.info(df.head(3).to_string())
        
        # Vérifier les types
        logger.info("\n🔍 TYPES DE DONNÉES:")
        logger.info(df.dtypes)
        
        # Vérifier les valeurs uniques dans les colonnes clés
        logger.info("\n🔍 VALEURS UNIQUES:")
        for col in df.columns:
            if col in ['annee', 'nb_ventes', 'prix_m2_median', 'dpe_total']:
                unique_vals = df[col].unique()
                logger.info(f"  {col}: {sorted(unique_vals)}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Erreur debug GOLD: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

if __name__ == "__main__":
    debug_gold_structure()
