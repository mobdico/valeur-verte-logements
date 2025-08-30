 #!/usr/bin/env python3
"""
Script de configuration de l'environnement de développement
Test de connexion MinIO et création des dossiers de base
"""

import os
import sys
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_minio_connection():
    """Test de connexion à MinIO"""
    try:
        # Configuration MinIO
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
        secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
        
        # Client S3 compatible MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{minio_endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'  # MinIO utilise cette région par défaut
        )
        
        # Test de connexion
        response = s3_client.list_buckets()
        logger.info("✅ Connexion MinIO réussie !")
        logger.info(f"Buckets disponibles : {[b['Name'] for b in response['Buckets']]}")
        return True
        
    except ClientError as e:
        logger.error(f"❌ Erreur de connexion MinIO : {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        return False

def create_local_directories():
    """Création des répertoires locaux nécessaires"""
    directories = [
        'data/raw',
        'data/bronze',
        'data/silver', 
        'data/gold',
        'logs',
        'notebooks',
        'scripts'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Répertoire créé : {directory}")

def main():
    """Fonction principale"""
    logger.info("🚀 Configuration de l'environnement de développement...")
    
    # Création des répertoires
    create_local_directories()
    
    # Test de connexion MinIO
    if test_minio_connection():
        logger.info("✅ Environnement configuré avec succès !")
        return 0
    else:
        logger.error("❌ Échec de la configuration")
        return 1

if __name__ == "__main__":
    sys.exit(main())
