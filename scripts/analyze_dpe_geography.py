#!/usr/bin/env python3
"""
Script d'analyse de la répartition géographique des classes DPE
Utilise boto3 pour éviter les problèmes PyArrow
"""

import boto3
import pandas as pd
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_dpe_geography():
    """Analyse la répartition géographique des classes DPE par département"""
    logger.info("🗺️ ANALYSE RÉPARTITION GÉOGRAPHIQUE DPE")
    logger.info("=" * 60)
    
    try:
        # Connexion MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )
        
        # Lister les fichiers DPE Silver
        bucket = "datalake-silver"
        prefix = "dpe/"
        
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        dpe_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
        
        logger.info(f"📁 Fichiers DPE trouvés: {len(dpe_files)}")
        
        # Analyser tous les départements
        deps_interet = ['92', '59', '34']
        all_dpe_data = []
        
        for dept in deps_interet:
            logger.info(f"\n🔍 ANALYSE DÉPARTEMENT {dept}")
            
            # Chercher les fichiers pour ce département
            dept_files = [f for f in dpe_files if f"departement_code={dept}" in f]
            
            if dept_files:
                logger.info(f"  📂 Fichiers trouvés: {len(dept_files)}")
                
                # Lire le premier fichier du département
                sample_file = dept_files[0]
                logger.info(f"  📖 Lecture: {sample_file}")
                
                # Télécharger et lire le fichier
                obj = s3_client.get_object(Bucket=bucket, Key=sample_file)
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                
                logger.info(f"  📊 Lignes: {len(df):,}")
                logger.info(f"  🏷️ Colonnes: {list(df.columns)}")
                
                # Vérifier la colonne classe_consommation_energie
                if 'classe_consommation_energie' in df.columns:
                    col_name = 'classe_consommation_energie'
                elif 'classe_consommation_enerrgie' in df.columns:  # Faute de frappe
                    col_name = 'classe_consommation_enerrgie'
                else:
                    logger.error(f"  ❌ Colonne classe_consommation_energie non trouvée")
                    continue
                
                # Distribution des classes par département
                dept_dist = df[col_name].value_counts().sort_index()
                logger.info(f"  🏷️ DISTRIBUTION CLASSES:")
                
                for classe, count in dept_dist.items():
                    if pd.notna(classe):  # Éviter les NaN
                        percentage = (count / len(df) * 100)
                        logger.info(f"    {classe}: {count:,} ({percentage:.2f}%)")
                
                # Focus sur les passoires (F, G)
                passoires = df[df[col_name].isin(['F', 'G'])]
                if len(passoires) > 0:
                    pct_passoires = (len(passoires) / len(df) * 100)
                    logger.info(f"  🏚️ Passoires (F+G): {len(passoires):,} ({pct_passoires:.2f}%)")
                else:
                    logger.info(f"  🏚️ Passoires (F+G): 0 (0%)")
                
                all_dpe_data.append({
                    'departement': dept,
                    'data': df,
                    'col_name': col_name,
                    'total': len(df),
                    'passoires': len(passoires)
                })
                
            else:
                logger.info(f"  ❌ Aucun fichier trouvé pour le département {dept}")
        
        # Analyse globale
        if all_dpe_data:
            logger.info(f"\n📊 ANALYSE GLOBALE:")
            total_global = sum(item['total'] for item in all_dpe_data)
            total_passoires_global = sum(item['passoires'] for item in all_dpe_data)
            
            logger.info(f"  Total DPE analysés: {total_global:,}")
            logger.info(f"  Total passoires (F+G): {total_passoires_global:,}")
            logger.info(f"  % passoires global: {(total_passoires_global/total_global*100):.2f}%")
            
            # Distribution globale des classes
            logger.info(f"\n🏷️ DISTRIBUTION GLOBALE CLASSES DPE:")
            all_classes = {}
            for item in all_dpe_data:
                df = item['data']
                col_name = item['col_name']
                dept_dist = df[col_name].value_counts()
                
                for classe, count in dept_dist.items():
                    if pd.notna(classe):
                        if classe in all_classes:
                            all_classes[classe] += count
                        else:
                            all_classes[classe] = count
            
            # Trier et afficher
            for classe in sorted(all_classes.keys()):
                count = all_classes[classe]
                percentage = (count / total_global * 100)
                logger.info(f"  {classe}: {count:,} ({percentage:.2f}%)")
        
        return all_dpe_data
        
    except Exception as e:
        logger.error(f"❌ Erreur analyse géographique DPE: {e}")
        return None

if __name__ == "__main__":
    analyze_dpe_geography()
