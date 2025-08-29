#!/usr/bin/env python3
"""
Script d'analyse de la qualité des données GOLD
- Vérifie les agrégations DVF et DPE
- Analyse la distribution des indicateurs
- Valide la structure des données
"""

import logging
import pandas as pd
import boto3
import pyarrow.dataset as ds
import pyarrow.fs as pafs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_gold_quality():
    """Analyse la qualité des données GOLD"""
    logger.info("🔍 ANALYSE QUALITÉ DONNÉES GOLD")
    logger.info("=" * 60)
    
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
        # Lire le fichier complet au lieu du dataset partitionné
        gold_file = "datalake-gold/market_indicators/gold_complete.parquet"
        gold_df = pd.read_parquet(gold_file, filesystem=pa_fs)
        
        logger.info(f"📊 GOLD: {len(gold_df):,} lignes")
        logger.info(f"🏷️ Colonnes: {list(gold_df.columns)}")
        
        # 1. Vérification de la structure
        logger.info("\n🏗️ VÉRIFICATION STRUCTURE")
        logger.info("=" * 40)
        
        # Colonnes attendues
        expected_cols = [
            'departement', 'annee', 'trimestre', 'nb_ventes', 
            'prix_m2_median', 'prix_m2_mean', 'dpe_total'
        ]
        
        missing_cols = [col for col in expected_cols if col not in gold_df.columns]
        if missing_cols:
            logger.warning(f"⚠️ Colonnes manquantes: {missing_cols}")
        else:
            logger.info("✅ Toutes les colonnes attendues sont présentes")
        
        # 2. Analyse des départements
        logger.info("\n🗺️ ANALYSE DÉPARTEMENTS")
        logger.info("=" * 40)
        
        deps = gold_df['departement'].unique()
        logger.info(f"Départements: {sorted(deps)}")
        
        for dept in sorted(deps):
            dept_data = gold_df[gold_df['departement'] == dept]
            logger.info(f"  {dept}: {len(dept_data)} lignes")
        
        # 3. Analyse temporelle
        logger.info("\n📅 ANALYSE TEMPORELLE")
        logger.info("=" * 40)
        
        trimestres = gold_df['trimestre'].unique()
        logger.info(f"Trimestres: {sorted(trimestres)}")
        
        annees = gold_df['annee'].unique()
        logger.info(f"Années: {sorted(annees)}")
        
        # 4. Analyse des métriques DVF
        logger.info("\n💰 ANALYSE MÉTRIQUES DVF")
        logger.info("=" * 40)
        
        if 'nb_ventes' in gold_df.columns:
            logger.info(f"Nombre de ventes:")
            logger.info(f"  Min: {gold_df['nb_ventes'].min():,}")
            logger.info(f"  Max: {gold_df['nb_ventes'].max():,}")
            logger.info(f"  Moyenne: {gold_df['nb_ventes'].mean():.0f}")
            logger.info(f"  Total: {gold_df['nb_ventes'].sum():,}")
        
        if 'prix_m2_median' in gold_df.columns:
            logger.info(f"Prix/m² médian:")
            logger.info(f"  Min: {gold_df['prix_m2_median'].min():,} €")
            logger.info(f"  Max: {gold_df['prix_m2_median'].max():,} €")
            logger.info(f"  Moyenne: {gold_df['prix_m2_median'].mean():.0f} €")
        
        if 'prix_m2_mean' in gold_df.columns:
            logger.info(f"Prix/m² moyen:")
            logger.info(f"  Min: {gold_df['prix_m2_mean'].min():,} €")
            logger.info(f"  Max: {gold_df['prix_m2_mean'].max():,} €")
            logger.info(f"  Moyenne: {gold_df['prix_m2_mean'].mean():.0f} €")
        
        # 5. Analyse des métriques DPE
        logger.info("\n🏷️ ANALYSE MÉTRIQUES DPE")
        logger.info("=" * 40)
        
        # Colonnes DPE
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_')]
        logger.info(f"Colonnes DPE: {dpe_cols}")
        
        if 'dpe_total' in gold_df.columns:
            logger.info(f"Total DPE:")
            logger.info(f"  Min: {gold_df['dpe_total'].min():,}")
            logger.info(f"  Max: {gold_df['dpe_total'].max():,}")
            logger.info(f"  Moyenne: {gold_df['dpe_total'].mean():.0f}")
            logger.info(f"  Total global: {gold_df['dpe_total'].sum():,}")
        
        # 6. Vérification de la cohérence
        logger.info("\n🔍 VÉRIFICATION COHÉRENCE")
        logger.info("=" * 40)
        
        # Vérifier que chaque département a des données pour chaque trimestre
        for dept in sorted(deps):
            dept_data = gold_df[gold_df['departement'] == dept]
            dept_trimestres = dept_data['trimestre'].unique()
            logger.info(f"  {dept}: {len(dept_trimestres)} trimestres sur {len(trimestres)} attendus")
            
            if len(dept_trimestres) < len(trimestres):
                missing = set(trimestres) - set(dept_trimestres)
                logger.warning(f"    ⚠️ Trimestres manquants: {sorted(missing)}")
        
        # 7. Exemples de données
        logger.info("\n📋 EXEMPLES DE DONNÉES")
        logger.info("=" * 40)
        
        sample = gold_df.head(3)
        for idx, row in sample.iterrows():
            logger.info(f"  Ligne {idx}:")
            logger.info(f"    Département: {row['departement']}")
            logger.info(f"    Trimestre: {row['trimestre']}")
            if 'nb_ventes' in row:
                logger.info(f"    Ventes: {row['nb_ventes']:,}")
            if 'prix_m2_median' in row:
                logger.info(f"    Prix/m² médian: {row['prix_m2_median']:,} €")
            if 'dpe_total' in row:
                logger.info(f"    Total DPE: {row['dpe_total']:,}")
        
        logger.info("\n✅ ANALYSE GOLD TERMINÉE")
        return gold_df
        
    except Exception as e:
        logger.error(f"❌ Erreur analyse GOLD: {e}")
        return None

if __name__ == "__main__":
    analyze_gold_quality()
