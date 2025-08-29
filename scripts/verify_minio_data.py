#!/usr/bin/env python3
"""
Script de vérification des données dans MinIO (BRONZE ET SILVER)
- Liste récursive avec pagination
- Regroupe par dossiers
- Vérifie la structure attendue (DPE & DVF)
"""

import boto3
from botocore.exceptions import ClientError
from datetime import datetime

BUCKET = "datalake-bronze"
SILVER_BUCKET = "datalake-silver"  # Nouveau bucket

def list_all_objects(s3_client, bucket, prefix=""):
    """Itère sur tous les objets du bucket (paginé)."""
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in page.get('Contents', []):
            yield c

def verify_minio_data():
    # Client MinIO (dans Docker)
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

    print("🔍 VÉRIFICATION COMPLÈTE DES DONNÉES MINIO (BRONZE)")
    print("=" * 70)

    try:
        # Test bucket
        s3_client.head_bucket(Bucket=BUCKET)
    except ClientError as e:
        print(f"❌ Bucket {BUCKET} introuvable ou inaccessible: {e}")
        return

    # Regroupement par dossiers
    folders = {}
    total_files = 0
    total_size = 0

    for obj in list_all_objects(s3_client, BUCKET):
        key = obj['Key']
        size = obj['Size']
        last_modified = obj['LastModified']

        parts = key.split('/')
        main_folder = parts[0] if len(parts) > 0 else key
        sub_folder = parts[1] if len(parts) > 1 else None

        folders.setdefault(main_folder, {})
        folders[main_folder].setdefault(sub_folder, [])
        folders[main_folder][sub_folder].append({
            'key': key,
            'size': size,
            'last_modified': last_modified
        })

        total_files += 1
        total_size += size

    if total_files == 0:
        print("❌ Bucket vide.")
        return

    # Affichage par dossier
    for main_folder, sub in folders.items():
        print(f"\n📂 {main_folder.upper()}/")
        print("-" * 40)

        main_files = 0
        main_size = 0

        for sub_folder, files in sub.items():
            if sub_folder:
                print(f"  📁 {sub_folder}/")

            sub_files = 0
            sub_size = 0

            for f in files:
                key = f['key']
                size = f['size']
                modified = f['last_modified']
                filename = key.split('/')[-1]

                print(f"    📄 {filename}")
                print(f"        💾 {size:,} bytes ({size/1024/1024:.2f} MB)")
                print(f"        📅 {modified.strftime('%Y-%m-%d %H:%M:%S')}")

                sub_files += 1
                sub_size += size

            if sub_folder:
                print(f"    📊 {sub_folder}/ : {sub_files} fichiers, {sub_size:,} bytes")

            main_files += sub_files
            main_size += sub_size

        print(f"  📊 {main_folder}/ : {main_files} fichiers, {main_size:,} bytes")

    # Stat globales
    print(f"\n📊 STATISTIQUES GLOBALES")
    print("=" * 70)
    print(f"📄 Total fichiers : {total_files}")
    print(f"💾 Taille totale  : {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    print(f"📅 Vérifié le     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Structure attendue
    print(f"\n✅ VÉRIFICATION DE LA STRUCTURE ATTENDUE")
    print("=" * 70)
    expected = {
        'dpe': ['92', '59', '34'],
        'dvf': ['2020', '2021']
    }
    for root, subs in expected.items():
        if root in folders:
            print(f"✅ {root}/ présent")
            for s in subs:
                if s in folders[root]:
                    print(f"  ✅ {root}/{s}/ présent")
                else:
                    print(f"  ❌ {root}/{s}/ MANQUANT")
        else:
            print(f"❌ {root}/ MANQUANT")

    print(f"\n🎯 BRONZE prêt si les dossiers attendus sont présents.")

def verify_silver_data():
    """Vérifie la structure et le contenu du bucket SILVER"""
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )
    
    print("\n🔍 VÉRIFICATION COMPLÈTE DES DONNÉES MINIO (SILVER)")
    print("=" * 70)
    
    try:
        s3_client.head_bucket(Bucket=SILVER_BUCKET)
    except ClientError as e:
        print(f"❌ Bucket {SILVER_BUCKET} introuvable ou inaccessible: {e}")
        return
    
    # Vérifier structure dvf/ et dpe/
    verify_silver_structure(s3_client)

def verify_silver_structure(s3_client):
    """Vérifie la structure des données SILVER"""
    print("📂 VÉRIFICATION STRUCTURE SILVER:")
    
    # Vérifier DVF
    dvf_objects = list(list_all_objects(s3_client, SILVER_BUCKET, "dvf/"))
    if dvf_objects:
        print("✅ DVF/ présent")
        print(f"  📊 Total objets DVF: {len(dvf_objects)}")
        for obj in dvf_objects[:5]:  # Premiers 5 objets
            print(f"    📄 {obj['Key']} - {obj['Size']:,} bytes")
        if len(dvf_objects) > 5:
            print(f"    ... et {len(dvf_objects) - 5} autres objets")
    else:
        print("❌ DVF/ absent")
    
    # Vérifier DPE
    dpe_objects = list(list_all_objects(s3_client, SILVER_BUCKET, "dpe/"))
    if dpe_objects:
        print("✅ DPE/ présent")
        print(f"  📊 Total objets DPE: {len(dpe_objects)}")
        for obj in dpe_objects[:5]:  # Premiers 5 objets
            print(f"    📄 {obj['Key']} - {obj['Size']:,} bytes")
        if len(dpe_objects) > 5:
            print(f"    ... et {len(dpe_objects) - 5} autres objets")
    else:
        print("❌ DPE/ absent")
    
    # Statistiques globales SILVER
    total_silver_objects = len(dvf_objects) + len(dpe_objects)
    total_silver_size = sum(obj['Size'] for obj in dvf_objects + dpe_objects)
    
    print(f"\n📊 STATISTIQUES SILVER")
    print("-" * 40)
    print(f"📄 Total objets: {total_silver_objects}")
    print(f"💾 Taille totale: {total_silver_size:,} bytes ({total_silver_size/1024/1024:.2f} MB)")
    
    if total_silver_objects > 0:
        print(f"\n🎯 SILVER prêt avec {total_silver_objects} objets transformés.")
    else:
        print(f"\n⚠️ SILVER vide - transformation non effectuée.")

if __name__ == "__main__":
    verify_minio_data()      # BRONZE
    verify_silver_data()     # SILVER
