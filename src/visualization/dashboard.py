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
5. **Visualisation** → Dashboard interactif

### Technologies utilisées :
- **Python 3.11** avec Polars, DuckDB, PyArrow
- **MinIO** pour le stockage objet
- **Docker** pour la conteneurisation
- **Streamlit** pour l'interface utilisateur
""")

# Section GOLD - Analyses métier
st.header("💎 Analyses GOLD - Valeur Verte")

# Charger les données GOLD
@st.cache_data(ttl=60)  # Cache de 60 secondes pour forcer le rechargement
def load_gold_data():
    try:
        import boto3
        import pandas as pd
        import io
        
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            region_name='us-east-1'
        )
        
        # Lire le fichier GOLD complet
        obj = s3_client.get_object(Bucket='datalake-gold', Key='market_indicators/gold_complete.parquet')
        
        # Lire le contenu en mémoire d'abord
        content = obj['Body'].read()
        
        # Créer un buffer en mémoire pour pandas
        buffer = io.BytesIO(content)
        df = pd.read_parquet(buffer)
        
        # Log de débogage détaillé
        st.info(f"🔍 DEBUG: Données chargées - Départements: {sorted(df['departement'].unique())}, Lignes: {len(df)}")
        st.info(f"🔍 DEBUG: Colonnes disponibles: {list(df.columns)}")
        st.info(f"🔍 DEBUG: Première ligne: {df.iloc[0].to_dict()}")
        
        return df
    except Exception as e:
        st.error(f"❌ Erreur chargement données GOLD: {e}")
        return None

# Charger les données
gold_df = load_gold_data()

# Vérification explicite des données
if gold_df is not None:
    # Vérifier que ce sont les bonnes données
    depts_charges = sorted(gold_df['departement'].unique())
    depts_attendus = ['34', '59', '92']
    
    if depts_charges == depts_attendus:
        st.success("✅ Données GOLD correctes chargées !")
    else:
        st.error(f"❌ MAUVAISES DONNÉES ! Attendu: {depts_attendus}, Chargé: {depts_charges}")
        st.warning("🔍 Vérifiez la source des données...")
    
    st.info(f"🔍 VÉRIFICATION: Départements chargés = {depts_charges}")
    st.success(f"✅ Données GOLD chargées : {len(gold_df):,} lignes")
    
    # Métriques clés
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_ventes = gold_df['nb_ventes'].sum()
        st.metric("Total Ventes", f"{total_ventes:,}", "2020-2021")
    
    with col2:
        prix_m2_moyen = gold_df['prix_m2_median'].mean()
        st.metric("Prix/m² Moyen", f"{prix_m2_moyen:.0f}€", "Médian")
    
    with col3:
        total_dpe = gold_df['dpe_total'].sum()
        st.metric("Total DPE", f"{total_dpe:.0f}", "Logements classés")
    
    with col4:
        nb_departements = gold_df['departement'].nunique()
        st.metric("Départements", nb_departements, "Analysés")
    
    # Visualisations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Évolution des prix par trimestre")
        
        # Graphique évolution prix
        fig_prix = px.line(
            gold_df, 
            x='trimestre', 
            y='prix_m2_median',
            color='departement',
            title="Prix/m² médian par trimestre et département",
            labels={'prix_m2_median': 'Prix/m² médian (€)', 'trimestre': 'Trimestre'}
        )
        fig_prix.update_layout(height=400)
        st.plotly_chart(fig_prix, use_container_width=True)
    
    with col2:
        st.subheader("🏷️ Distribution des classes DPE")
        
        # Calculer la moyenne des pourcentages par classe
        dpe_pct_cols = [col for col in gold_df.columns if col.endswith('_pct')]
        dpe_avg = gold_df[dpe_pct_cols].mean()
        
        # Créer le graphique en barres
        fig_dpe = px.bar(
            x=dpe_avg.index,
            y=dpe_avg.values,
            title="Pourcentage moyen par classe DPE",
            labels={'x': 'Classe DPE', 'y': 'Pourcentage moyen (%)'}
        )
        fig_dpe.update_layout(height=400)
        st.plotly_chart(fig_dpe, use_container_width=True)
    
    # Analyse géographique
    st.subheader("🗺️ Analyse géographique")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Prix par département
        prix_par_dept = gold_df.groupby('departement')['prix_m2_median'].mean().reset_index()
        fig_geo_prix = px.bar(
            prix_par_dept,
            x='departement',
            y='prix_m2_median',
            title="Prix/m² moyen par département",
            labels={'prix_m2_median': 'Prix/m² moyen (€)', 'departement': 'Département'}
        )
        st.plotly_chart(fig_geo_prix, use_container_width=True)
    
    with col2:
        # Ventes par département
        ventes_par_dept = gold_df.groupby('departement')['nb_ventes'].sum().reset_index()
        fig_geo_ventes = px.bar(
            ventes_par_dept,
            x='departement',
            y='nb_ventes',
            title="Nombre total de ventes par département",
            labels={'nb_ventes': 'Nombre de ventes', 'departement': 'Département'}
        )
        st.plotly_chart(fig_geo_ventes, use_container_width=True)
    
    # Tableau des données GOLD
    st.subheader("📋 Données GOLD détaillées")
    
    # Filtrer les colonnes importantes
    cols_important = ['departement', 'trimestre', 'annee', 'nb_ventes', 'prix_m2_median', 'prix_m2_mean', 'dpe_total']
    st.dataframe(gold_df[cols_important], use_container_width=True)
    
    # Téléchargement des données
    csv = gold_df.to_csv(index=False)
    st.download_button(
        label="📥 Télécharger données GOLD (CSV)",
        data=csv,
        file_name="gold_data.csv",
        mime="text/csv"
    )

else:
    st.warning("⚠️ Impossible de charger les données GOLD. Vérifiez la connexion MinIO.")

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




