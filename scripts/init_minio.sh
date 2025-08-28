#!/bin/bash

# Script d'initialisation de MinIO
# Création des buckets et configuration des politiques

set -e

echo "🚀 Initialisation de MinIO..."

# Attendre que MinIO soit prêt
echo "⏳ Attente que MinIO soit prêt..."
until curl -s http://minio:9000/minio/health/live; do
    echo "MinIO pas encore prêt, attente..."
    sleep 2
done

echo "✅ MinIO est prêt !"

# Configuration de l'alias MinIO
echo "🔧 Configuration de l'alias MinIO..."
mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Création des buckets
echo "📦 Création des buckets..."
mc mb myminio/datalake-bronze
mc mb myminio/datalake-silver
mc mb myminio/datalake-gold

# Configuration des politiques (lecture publique pour la démo)
echo "🔐 Configuration des politiques..."
mc policy set public myminio/datalake-bronze
mc policy set public myminio/datalake-silver
mc policy set public myminio/datalake-gold

# Création des dossiers de base
echo "📁 Création des dossiers de base..."
mc cp --recursive /scripts/sample_data/ myminio/datalake-bronze/

echo "✅ Initialisation de MinIO terminée !"
echo "📊 Buckets créés :"
mc ls myminio/
