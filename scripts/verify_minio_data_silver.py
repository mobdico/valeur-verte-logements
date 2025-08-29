#!/usr/bin/env python3
"""
Script de vérification des données dans MinIO (SILVER)
- Liste récursive avec pagination
- Regroupe par dossiers
- Vérifie la structure attendue (DPE & DVF transformés)
"""

import boto3
from botocore.exceptions import ClientError
from datetime import datetime

BUCKET = "datalake-silver"

def list_all_objects(s3_client, bucket, prefix=""):
    """Itère sur tous les objets du bucket (paginé)."""
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in page.get('Contents', []):
            yield c

def verify_minio_data_silver():
    # Client MinIO (dans Docker)
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

    print("🔍 VÉRIFICATION COMPLÈTE DES DONNÉES MINIO (SILVER)")
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

    # Structure attendue pour SILVER
    print(f"\n✅ VÉRIFICATION DE LA STRUCTURE ATTENDUE (SILVER)")
    print("=" * 70)
    expected = {
        'dpe': ['departement_code=92', 'departement_code=59', 'departement_code=34'],
        'dvf': ['annee=2020', 'annee=2021']
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

    # Vérification des colonnes attendues
    print(f"\n🔍 VÉRIFICATION DES COLONNES ATTENDUES")
    print("=" * 70)
    
    # Vérifier un échantillon DPE
    dpe_files = [f for f in list_all_objects(s3_client, BUCKET, "dpe/") if f['Key'].endswith('.parquet')]
    if dpe_files:
        sample_dpe = dpe_files[0]['Key']
        print(f"📖 Échantillon DPE: {sample_dpe}")
        try:
            import pandas as pd
            obj = s3_client.get_object(Bucket=BUCKET, Key=sample_dpe)
            df = pd.read_parquet(obj['Body'])
            print(f"  📊 Colonnes DPE: {list(df.columns)}")
            print(f"  📊 Lignes: {len(df):,}")
        except Exception as e:
            print(f"  ❌ Erreur lecture: {e}")
    
    # Vérifier un échantillon DVF
    dvf_files = [f for f in list_all_objects(s3_client, BUCKET, "dvf/") if f['Key'].endswith('.parquet')]
    if dvf_files:
        sample_dvf = dvf_files[0]['Key']
        print(f"📖 Échantillon DVF: {sample_dvf}")
        try:
            import pandas as pd
            obj = s3_client.get_object(Bucket=BUCKET, Key=sample_dvf)
            df = pd.read_parquet(obj['Body'])
            print(f"  📊 Colonnes DVF: {list(df.columns)}")
            print(f"  📊 Lignes: {len(df):,}")
        except Exception as e:
            print(f"  ❌ Erreur lecture: {e}")

    print(f"\n🎯 SILVER prêt si les dossiers attendus sont présents et les colonnes correctes.")

if __name__ == "__main__":
    verify_minio_data_silver()
