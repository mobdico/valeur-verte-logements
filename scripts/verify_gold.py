#!/usr/bin/env python3
"""
Script de vérification rapide du bucket GOLD
"""

import boto3
from botocore.exceptions import ClientError

def verify_gold():
    # Client MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )
    
    bucket = "datalake-gold"
    
    print("🔍 VÉRIFICATION BUCKET GOLD")
    print("=" * 50)
    
    try:
        # Lister les objets
        response = s3_client.list_objects_v2(Bucket=bucket)
        
        if 'Contents' in response:
            print(f"✅ Bucket {bucket} accessible")
            print(f"📄 Nombre d'objets: {len(response['Contents'])}")
            
            for obj in response['Contents']:
                key = obj['Key']
                size = obj['Size']
                print(f"  📄 {key} ({size:,} bytes)")
        else:
            print(f"⚠️ Bucket {bucket} vide")
            
    except ClientError as e:
        print(f"❌ Erreur accès bucket {bucket}: {e}")

if __name__ == "__main__":
    verify_gold()
