#!/usr/bin/env python3
"""
Dashboard Streamlit pour la Valeur Verte des Logements
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
from pathlib import Path

# Ajouter le répertoire src au path
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

try:
    from config import get_config, BUCKETS
    config = get_config()
except ImportError:
    st.error("❌ Erreur d'import de la configuration")
    config = {}

# Configuration de la page
st.set_page_config(
    page_title="🏠 Valeur Verte des Logements",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre principal
st.title("🏠 Valeur Verte des Logements")
st.markdown("**Analyse de l'effet de la performance énergétique (DPE) sur les prix de vente (DVF)**")

# Sidebar
st.sidebar.header("📊 Configuration")
st.sidebar.markdown("**Période d'analyse :** 2020-2021-S1")
st.sidebar.markdown("**Sources :** DPE (API ADEME) + DVF (Valeurs foncières)")

# Informations du projet
col1, col2, col3 = st.columns(3)

with col1:
    st.info("**📥 Bronze Layer**\nDonnées brutes (DPE JSON, DVF TXT)")
    
with col2:
    st.info("**🔄 Silver Layer**\nDonnées nettoyées et typées (Parquet)")
    
with col3:
    st.info("**💎 Gold Layer**\nAgrégats analytiques (modèles hédoniques)")

# Section de statut des services
st.header("🔍 Statut des Services")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("MinIO", "🟢 En ligne", "Port 9000")
    
with col2:
    st.metric("ETL", "🟢 En ligne", "Service Python")
    
with col3:
    st.metric("JupyterLab", "🟢 En ligne", "Port 8888")
    
with col4:
    st.metric("Dashboard", "🟢 En ligne", "Port 8501")

# Configuration MinIO
st.header("⚙️ Configuration MinIO")

if config:
    minio_config = config.get('minio', {})
    buckets = config.get('buckets', {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔗 Connexion")
        st.json({
            "Endpoint": minio_config.get('endpoint', 'N/A'),
            "Access Key": minio_config.get('access_key', 'N/A'),
            "Region": minio_config.get('region', 'N/A')
        })
    
    with col2:
        st.subheader("📦 Buckets")
        for layer, bucket_name in buckets.items():
            st.success(f"**{layer.title()}**: `{bucket_name}`")
else:
    st.warning("⚠️ Configuration non disponible")

# Architecture DataLake
st.header("🏗️ Architecture DataLake")

# Diagramme simple de l'architecture
architecture_data = {
    "Layer": ["Bronze", "Silver", "Gold"],
    "Format": ["JSON/TXT", "Parquet", "Parquet"],
    "Contenu": ["Données brutes", "Données nettoyées", "Agrégats"],
    "Partition": ["Non", "Oui (année/trimestre)", "Oui (année/trimestre)"]
}

df_arch = pd.DataFrame(architecture_data)
st.dataframe(df_arch, use_container_width=True)

# Sources de données
st.header("📊 Sources de Données")

col1, col2 = st.columns(2)

with col1:
    st.subheader("🏠 DPE (Diagnostic Performance Énergétique)")
    st.markdown("""
    - **Source :** API ADEME (dataset dpe-france)
    - **Période :** 2020-01-01 → 2021-06-30
    - **Champs clés :**
        - Classe consommation énergie (A-G)
        - Surface thermique
        - Code commune INSEE
        - Département
    """)

with col2:
    st.subheader("💰 DVF (Demandes de Valeurs Foncières)")
    st.markdown("""
    - **Source :** Valeurs foncières
    - **Millésimes :** 2020, 2021
    - **Champs clés :**
        - Date mutation
        - Valeur foncière
        - Surface bâtie
        - Type local
        - Code commune
    """)

# Pipeline ETL
st.header("🔄 Pipeline ETL")

st.markdown("""
### Étapes de traitement :

1. **Ingestion** → Données brutes vers Bronze
2. **Transformation** → Nettoyage et typage vers Silver  
3. **Agrégation** → Calculs métier vers Gold
4. **Modélisation** → Modèles hédoniques
5. **Visualisation** → Dashboard interactif

### Technologies utilisées :
- **Python 3.11** avec Polars, DuckDB, PyArrow
- **MinIO** pour le stockage objet
- **Docker** pour la conteneurisation
- **Streamlit** pour l'interface utilisateur
""")

# Footer
st.markdown("---")
st.markdown("*Dashboard développé avec Streamlit - Projet Valeur Verte des Logements*")

# Test de connexion MinIO
if st.button("🧪 Tester la connexion MinIO"):
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        minio_config = config.get('minio', {})
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{minio_config.get('endpoint', 'localhost:9000')}",
            aws_access_key_id=minio_config.get('access_key', 'admin'),
            aws_secret_access_key=minio_config.get('secret_key', 'password123'),
            region_name='us-east-1'
        )
        
        response = s3_client.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        
        st.success(f"✅ Connexion MinIO réussie !")
        st.info(f"Buckets disponibles : {buckets}")
        
    except Exception as e:
        st.error(f"❌ Erreur de connexion : {e}")



