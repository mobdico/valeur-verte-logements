#!/usr/bin/env python3
"""
Script de vérification des données dans MinIO
Vérifie la structure et le contenu des données DPE et DVF ingérées
"""

import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime

def verify_minio_data():
    """Vérifie toutes les données dans MinIO"""
    
    # Configuration MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )
    
    print("🔍 VÉRIFICATION COMPLÈTE DES DONNÉES MINIO")
    print("=" * 60)
    
    try:
        # Lister tous les objets dans le bucket
        response = s3_client.list_objects_v2(Bucket='datalake-bronze')
        
        if 'Contents' not in response:
            print("❌ Bucket vide ou erreur")
            return
        
        # Grouper par dossier
        folders = {}
        for obj in response['Contents']:
            key = obj['Key']
            size = obj['Size']
            last_modified = obj['LastModified']
            
            # Extraire le dossier principal et sous-dossier
            parts = key.split('/')
            main_folder = parts[0] if len(parts) > 0 else key
            sub_folder = parts[1] if len(parts) > 1 else None
            
            if main_folder not in folders:
                folders[main_folder] = {}
            
            if sub_folder not in folders[main_folder]:
                folders[main_folder][sub_folder] = []
            
            folders[main_folder][sub_folder].append({
                'key': key,
                'size': size,
                'last_modified': last_modified
            })
        
        # Afficher par dossier
        total_files = 0
        total_size = 0
        
        for main_folder, sub_folders in folders.items():
            print(f"\n📂 {main_folder.upper()}/")
            print("-" * 40)
            
            main_folder_files = 0
            main_folder_size = 0
            
            for sub_folder, files in sub_folders.items():
                if sub_folder:
                    print(f"  📁 {sub_folder}/")
                
                sub_folder_files = 0
                sub_folder_size = 0
                
                for file_info in files:
                    key = file_info['key']
                    size = file_info['size']
                    modified = file_info['last_modified']
                    
                    # Afficher le nom du fichier (sans le chemin complet)
                    filename = key.split('/')[-1]
                    print(f"    📄 {filename}")
                    print(f"        💾 Taille: {size:,} bytes ({size/1024/1024:.2f} MB)")
                    print(f"        📅 Modifié: {modified.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    sub_folder_files += 1
                    sub_folder_size += size
                
                if sub_folder:
                    print(f"    📊 Sous-dossier {sub_folder}: {sub_folder_files} fichiers, {sub_folder_size:,} bytes")
                
                main_folder_files += sub_folder_files
                main_folder_size += sub_folder_size
            
            print(f"  📊 Dossier {main_folder}: {main_folder_files} fichiers, {main_folder_size:,} bytes")
            
            total_files += main_folder_files
            total_size += main_folder_size
        
        # Statistiques globales
        print(f"\n📊 STATISTIQUES GLOBALES")
        print("=" * 60)
        print(f"📄 Total fichiers: {total_files}")
        print(f"💾 Taille totale: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        print(f"📅 Vérification effectuée: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Vérification de la structure attendue
        print(f"\n✅ VÉRIFICATION DE LA STRUCTURE")
        print("=" * 60)
        
        expected_structure = {
            'dpe': ['92', '59', '34'],
            'dvf': ['2020', '2021']
        }
        
        for expected_folder, expected_subfolders in expected_structure.items():
            if expected_folder in folders:
                print(f"✅ {expected_folder}/ - Présent")
                for subfolder in expected_subfolders:
                    if subfolder in folders[expected_folder]:
                        print(f"  ✅ {expected_folder}/{subfolder}/ - Présent")
                    else:
                        print(f"  ❌ {expected_folder}/{subfolder}/ - MANQUANT")
            else:
                print(f"❌ {expected_folder}/ - MANQUANT")
        
        print(f"\n🎯 PARTIE B - INGESTION BRONZE: {'✅ TERMINÉE' if total_files >= 8 else '❌ INCOMPLÈTE'}")
        
    except ClientError as e:
        print(f"❌ Erreur MinIO: {e}")
    except Exception as e:
        print(f"❌ Erreur générale: {e}")

if __name__ == "__main__":
    verify_minio_data()
