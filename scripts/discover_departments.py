#!/usr/bin/env python3
"""
Script pour découvrir tous les départements disponibles dans les données DPE
"""

import boto3
import pandas as pd
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def discover_all_departments():
    """Découvre tous les départements disponibles dans les données DPE"""
    logger.info("🔍 DÉCOUVERTE DE TOUS LES DÉPARTEMENTS DPE")
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
        
        # Extraire tous les codes département
        all_departments = set()
        department_stats = {}
        
        for file_key in dpe_files:
            # Extraire le code département du nom de fichier
            if "tv016_departement_code=" in file_key:
                # Format: dpe/tv016_departement_code=XX/annee=YYYY/trimestre=YYYYQX/...
                parts = file_key.split('/')
                for part in parts:
                    if part.startswith('tv016_departement_code='):
                        dept_code = part.split('=')[1]
                        all_departments.add(dept_code)
                        
                        # Compter les fichiers par département
                        if dept_code not in department_stats:
                            department_stats[dept_code] = {'files': 0, 'total_lines': 0}
                        department_stats[dept_code]['files'] += 1
                        break
        
        # Trier les départements
        sorted_departments = sorted(all_departments)
        
        logger.info(f"\n🏛️ DÉPARTEMENTS DÉCOUVERTS: {len(sorted_departments)}")
        logger.info("=" * 50)
        
        for dept in sorted_departments:
            logger.info(f"  {dept}: {department_stats[dept]['files']} fichiers")
        
        # Analyser quelques départements en détail
        logger.info(f"\n🔍 ANALYSE DÉTAILLÉE (échantillon de 5 départements)")
        logger.info("=" * 60)
        
        sample_departments = sorted_departments[:5]  # Premier 5
        
        for dept in sample_departments:
            logger.info(f"\n🏛️ DÉPARTEMENT {dept}:")
            
            # Trouver un fichier pour ce département
            dept_files = [f for f in dpe_files if f"tv016_departement_code={dept}" in f]
            
            if dept_files:
                sample_file = dept_files[0]
                logger.info(f"  📖 Lecture: {sample_file}")
                
                try:
                    # Lire le fichier
                    obj = s3_client.get_object(Bucket=bucket, Key=sample_file)
                    df = pd.read_parquet(BytesIO(obj['Body'].read()))
                    
                    logger.info(f"  📊 Lignes: {len(df):,}")
                    
                    # Vérifier la colonne classe_consommation_energie
                    if 'classe_consommation_energie' in df.columns:
                        col_name = 'classe_consommation_energie'
                    elif 'classe_consommation_ennergie' in df.columns:
                        col_name = 'classe_consommation_ennergie'
                    else:
                        logger.error(f"  ❌ Colonne classe_consommation_energie non trouvée")
                        continue
                    
                    # Distribution des classes
                    dept_dist = df[col_name].value_counts().sort_index()
                    logger.info(f"  🏷️ Classes disponibles: {list(dept_dist.index)}")
                    
                    # Focus sur les passoires (F, G)
                    passoires = df[df[col_name].isin(['F', 'G'])]
                    if len(passoires) > 0:
                        pct_passoires = (len(passoires) / len(df) * 100)
                        logger.info(f"  🏚️ Passoires (F+G): {len(passoires):,} ({pct_passoires:.2f}%)")
                    else:
                        logger.info(f"  🏚️ Passoires (F+G): 0 (0%)")
                    
                    # Mettre à jour les statistiques
                    department_stats[dept]['total_lines'] = len(df)
                    
                except Exception as e:
                    logger.error(f"  ❌ Erreur lecture fichier: {e}")
            else:
                logger.info(f"  ❌ Aucun fichier trouvé")
        
        # Résumé des statistiques
        logger.info(f"\n📊 RÉSUMÉ DES STATISTIQUES")
        logger.info("=" * 40)
        
        total_files = sum(stats['files'] for stats in department_stats.values())
        total_lines = sum(stats['total_lines'] for stats in department_stats.values())
        
        logger.info(f"  Total fichiers: {total_files}")
        logger.info(f"  Total lignes analysées: {total_lines:,}")
        logger.info(f"  Départements avec données: {len([d for d, stats in department_stats.items() if stats['total_lines'] > 0])}")
        
        return sorted_departments, department_stats
        
    except Exception as e:
        logger.error(f"❌ Erreur découverte départements: {e}")
        return None, None

if __name__ == "__main__":
    discover_all_departments()
