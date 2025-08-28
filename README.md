# 🏠 Valeur Verte des Logements

## 📋 Description

Projet d'analyse de l'effet de la performance énergétique (DPE) sur les prix de vente (DVF) à l'échelle locale, sur la fenêtre 2020-2021-S1.

## 🏗️ Architecture DataLake

- **Bronze** : Données brutes (DPE API JSON, DVF TXT)
- **Silver** : Données nettoyées et typées (Parquet partitionné)
- **Gold** : Agrégats analytiques (modèles hédoniques)

## 🚀 Démarrage rapide

### Prérequis
- Docker et Docker Compose installés
- Git

### Installation
```bash
# Cloner le projet
git clone <repository-url>
cd valeur-verte-logements

# Démarrer le cluster
make up

# Vérifier le statut
make status
```

### Accès aux services
- **MinIO Console** : http://localhost:9001 (admin/password123)
- **JupyterLab** : http://localhost:8888
- **Dashboard Streamlit** : http://localhost:8501

## 📁 Structure du projet

```
valeur-verte-logements/
├── src/                    # Code source Python
│   ├── data/              # Modules d'ingestion et traitement
│   ├── models/            # Modèles hédoniques
│   ├── utils/             # Utilitaires
│   └── visualization/     # Dashboard et visualisations
├── data/                  # Données locales
├── notebooks/             # Notebooks Jupyter
├── scripts/               # Scripts utilitaires
├── docker-compose.yml     # Configuration Docker
├── Makefile               # Commandes de gestion
└── requirements.txt       # Dépendances Python
```

## 🛠️ Commandes utiles

```bash
make help          # Afficher l'aide
make up            # Démarrer le cluster
make down          # Arrêter le cluster
make logs          # Afficher les logs
make shell-etl     # Accéder au shell ETL
make status        # Statut des services
make health        # Vérifier la santé
make clean         # Nettoyer l'environnement
```

## 🔧 Configuration

Copier `env.example` vers `.env` et ajuster les variables :
```bash
cp env.example .env
```

## 📊 Sources de données

- **DPE** : API ADEME (dataset dpe-france)
- **DVF** : Valeurs foncières (format TXT pipe "|")

## 🎯 Livrables

- Pipeline ETL dockerisé
- Jeux de données Parquet (bronze/silver/gold)
- Modèle hédonique (décote/prime)
- Dashboard interactif

## 📝 Licence

[À définir]
