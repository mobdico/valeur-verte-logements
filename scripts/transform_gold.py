#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script GOLD: agrégations prêtes BI
- DVF: métriques marché (nb ventes, prix_m2 médian/moyen) par département x trimestre
- DPE: distribution classes A..G (comptes + %)
- Join DVF & DPE → GOLD parquet partitionné (departement, trimestre)

PATCH 2025-08:
- Lecture Silver via pyarrow.dataset (au lieu de pandas.read_parquet) pour éviter
  les erreurs de "directory markers" sur MinIO/S3.
- DVF: tolère 'code_departement' (nouvelle partition) ou 'Code departement'.
"""

import logging
import pandas as pd
import numpy as np
import boto3, botocore
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pyarrow.dataset as ds

# -------------------- Config --------------------
logging.basicConfig(
    level="INFO",
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password123"

BUCKET_SILVER = "datalake-silver"
BUCKET_GOLD = "datalake-gold"

SILVER_DVF_PREFIX = "dvf"   # parquet dataset partitionné
SILVER_DPE_PREFIX = "dpe"   # parquet dataset partitionné

CLASSES = ["A", "B", "C", "D", "E", "F", "G"]


# -------------------- S3 helpers --------------------
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=botocore.client.Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(name: str):
    s3 = s3_client()
    try:
        s3.head_bucket(Bucket=name)
        logger.info(f"Bucket {name} présent ")
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket", "NotFound"):
            logger.info(f"Bucket {name} absent → création…")
            s3.create_bucket(Bucket=name)
            logger.info(f"Bucket {name} créé ")
        else:
            raise


def pa_s3_fs() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        scheme="http",
    )


def write_parquet_partitioned(table: pa.Table, bucket: str, prefix: str, partition_cols: list[str]):
    ensure_bucket(bucket)
    fs = pa_s3_fs()
    root = f"{bucket}/{prefix}".rstrip("/")
    logger.info(f"Écriture GOLD → {root} (partitions={partition_cols})")
    
    # IMPORTANT: Pour garder les colonnes de partitionnement dans les données,
    # on écrit d'abord un fichier complet, puis on le partitionne
    # Cela permet d'avoir toutes les colonnes disponibles pour l'analyse
    
    # 1. Écrire un fichier complet avec toutes les colonnes
    complete_file = f"{root}/gold_complete.parquet"
    logger.info(f"Écriture fichier complet: {complete_file}")
    pq.write_table(table, complete_file, filesystem=fs, compression="snappy")
    
    # 2. Écrire aussi en partitionné pour la performance
    logger.info("Écriture partitionnée pour performance...")
    pq.write_to_dataset(
        table,
        root_path=root,
        filesystem=fs,
        partition_cols=partition_cols,
        existing_data_behavior="overwrite_or_ignore",
        compression="snappy",
        use_dictionary=True,
    )
    
    logger.info("Écriture GOLD OK (fichier complet + partitionné)")


# -------------------- Load Silver (robuste MinIO) --------------------
def load_silver_parquet(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Lit un dataset Parquet partitionné via Arrow Dataset (robuste S3/MinIO).
    """
    fs = pa_s3_fs()
    root = f"{bucket}/{prefix}".rstrip("/")
    logger.info(f"Lecture Silver (Arrow Dataset): {root}")
    dataset = ds.dataset(root, filesystem=fs, format="parquet", partitioning="hive")
    table = dataset.to_table()  # possibilité: columns=[...] si besoin
    df = table.to_pandas()
    logger.info(f"→ {len(df):,} lignes")
    return df


# -------------------- DVF agg --------------------
def agg_dvf(dvf: pd.DataFrame) -> pd.DataFrame:
    """
    Attend au moins:
      - 'code_departement' OU 'Code departement'
      - 'trimestre'
      - 'prix_m2'
    """
    dvf = dvf.copy()

    # Harmonisation code département
    if "code_departement" in dvf.columns:
        dvf["departement"] = dvf["code_departement"].astype(str)
    elif "Code departement" in dvf.columns:
        dvf["departement"] = dvf["Code departement"].astype(str)
    else:
        raise ValueError("Colonne département manquante: 'code_departement' ou 'Code departement'")

    needed = {"departement", "trimestre", "prix_m2"}
    missing = needed - set(dvf.columns)
    if missing:
        raise ValueError(f"Colonnes DVF manquantes: {missing}")

    dvf = dvf.dropna(subset=["departement", "trimestre", "prix_m2"])

    grp = dvf.groupby(["departement", "trimestre"], as_index=False).agg(
        nb_ventes=("prix_m2", "size"),
        prix_m2_median=("prix_m2", "median"),
        prix_m2_mean=("prix_m2", "mean"),
    )
    grp["prix_m2_median"] = grp["prix_m2_median"].round(0)
    grp["prix_m2_mean"] = grp["prix_m2_mean"].round(0)
    return grp


# -------------------- DPE agg --------------------
def agg_dpe(dpe: pd.DataFrame) -> pd.DataFrame:
    """
    Attend colonnes:
      - 'tv016_departement_code' (string), 'trimestre' (ex '2020Q1'),
        'classe_consommation_energie' in {'A'..'G'}
    Produit:
      - comptes par classe + pourcentages (classe_pct_X)
    """
    needed = {"tv016_departement_code", "trimestre", "classe_consommation_energie"}
    missing = needed - set(dpe.columns)
    if missing:
        raise ValueError(f"Colonnes DPE manquantes: {missing}")

    df = dpe.copy()
    df = df.dropna(subset=["tv016_departement_code", "trimestre", "classe_consommation_energie"])
    df["tv016_departement_code"] = df["tv016_departement_code"].astype(str)

    # Garder seulement A..G
    df = df[df["classe_consommation_energie"].isin(CLASSES)]

    # Comptage par classe
    counts = (
        df.groupby(["tv016_departement_code", "trimestre", "classe_consommation_energie"])
          .size()
          .reset_index(name="count")
    )

    # Pivot → colonnes classe_A, ..., classe_G
    pivot = counts.pivot_table(
        index=["tv016_departement_code", "trimestre"],
        columns="classe_consommation_energie",
        values="count",
        fill_value=0,
        aggfunc="sum",
    ).reset_index()

    pivot.columns = ["tv016_departement_code", "trimestre"] + [f"classe_{c}" for c in pivot.columns[2:]]

    # Total et pourcentages
    cls_cols = [f"classe_{c}" for c in CLASSES if f"classe_{c}" in pivot.columns]
    pivot["dpe_total"] = pivot[cls_cols].sum(axis=1)

    for c in CLASSES:
        col = f"classe_{c}"
        if col in pivot.columns:
            pivot[f"{col}_pct"] = np.where(
                pivot["dpe_total"] > 0,
                (pivot[col] / pivot["dpe_total"] * 100).round(1),
                0.0,
            )

    pivot = pivot.rename(columns={"tv016_departement_code": "departement"})
    return pivot


# -------------------- Build GOLD --------------------
def build_gold():
    # 1) Charger SILVER (robuste)
    dvf = load_silver_parquet(BUCKET_SILVER, SILVER_DVF_PREFIX)
    dpe = load_silver_parquet(BUCKET_SILVER, SILVER_DPE_PREFIX)

    # 2) Agrégations
    dvf_agg = agg_dvf(dvf)
    dpe_agg = agg_dpe(dpe)

    # 3) Join (departement, trimestre)
    gold = dvf_agg.merge(dpe_agg, on=["departement", "trimestre"], how="left")

    # 4) Ajouter 'annee' pour compat partition
    gold["annee"] = gold["trimestre"].str.slice(0, 4)

    # 5) Ordonner les colonnes principales
    ordered = ["departement", "annee", "trimestre", "nb_ventes", "prix_m2_median", "prix_m2_mean", "dpe_total"]
    cls_counts = [c for c in gold.columns if c.startswith("classe_") and not c.endswith("_pct")]
    cls_pcts   = [c for c in gold.columns if c.startswith("classe_") and c.endswith("_pct")]
    other_cols = [c for c in gold.columns if c not in ordered + cls_counts + cls_pcts]
    cols = ordered + sorted(cls_counts) + sorted(cls_pcts) + sorted(other_cols)
    gold = gold.reindex(columns=cols)

    logger.info(f"GOLD lignes: {len(gold):,}")

    # 6) Écriture parquet partitionné (departement, trimestre)
    table = pa.Table.from_pandas(gold, preserve_index=False)
    write_parquet_partitioned(
        table=table,
        bucket=BUCKET_GOLD,
        prefix="market_indicators",
        partition_cols=["departement", "trimestre"],
    )
    
    return gold  # Retourner le DataFrame pour les analyses


# -------------------- Business Analytics (Nos 9 questions) --------------------
def analyze_decote_verte(dvf_silver: pd.DataFrame, dpe_silver: pd.DataFrame) -> dict:
    """Question 1: Décote verte F-G vs D (contrôlée)"""
    logger.info("ANALYSE: Décote verte F-G vs D")
    
    try:
        # 1. Vérifier les classes DPE disponibles
        classes_disponibles = dpe_silver['classe_consommation_energie'].unique()
        logger.info(f"Classes DPE disponibles: {sorted(classes_disponibles)}")
        
        # 2. Jointure moins restrictive (seulement sur code commune)
        logger.info("Jointure DVF+DPE sur code commune...")
        merged = dvf_silver.merge(
            dpe_silver[['code_insee_commune_actualise', 'classe_consommation_energie']],
            left_on='Code commune',
            right_on='code_insee_commune_actualise',
            how='inner'
        )
        
        logger.info(f"Jointure DVF+DPE: {len(merged)} transactions")
        
        # 3. Vérifier les classes après jointure
        classes_apres_join = merged['classe_consommation_energie'].unique()
        logger.info(f"Classes après jointure: {sorted(classes_apres_join)}")
        
        # 4. Filtrer classes D, F, G (selon disponibilité)
        classes_target = ['D', 'F', 'G']
        classes_disponibles_filter = [c for c in classes_target if c in classes_apres_join]
        
        if not classes_disponibles_filter:
            logger.warning("Aucune des classes D, F, G n'est disponible")
            return {"status": "erreur", "message": "Classes D, F, G non disponibles"}
        
        logger.info(f"Classes cibles disponibles: {classes_disponibles_filter}")
        
        filtered = merged[merged['classe_consommation_energie'].isin(classes_disponibles_filter)]
        logger.info(f"Transactions filtrées: {len(filtered)}")
        
        # 5. Calculer prix/m² moyen par classe
        prix_par_classe = filtered.groupby('classe_consommation_energie')['prix_m2'].agg(['mean', 'count']).round(2)
        logger.info(f"Prix par classe:\n{prix_par_classe}")
        
        # 6. Calculer décote selon les classes disponibles
        results = {
            "status": "succès",
            "prix_m2_moyen": {},
            "decote_pourcentage": {},
            "nb_transactions": {},
            "classes_analysées": classes_disponibles_filter
        }
        
        # Remplir les résultats selon les classes disponibles
        for classe in classes_disponibles_filter:
            prix = prix_par_classe.loc[classe, 'mean']
            count = int(prix_par_classe.loc[classe, 'count'])
            
            results["prix_m2_moyen"][classe] = prix
            results["nb_transactions"][classe] = count
            
            # Calculer décote si classe D disponible
            if classe != 'D' and 'D' in classes_disponibles_filter:
                prix_d = prix_par_classe.loc['D', 'mean']
                decote = ((prix_d - prix) / prix_d * 100).round(2)
                results["decote_pourcentage"][f"{classe}_vs_D"] = decote
                logger.info(f"Décote {classe} vs D: {decote}%")
        
        logger.info(f"✅ Décote verte calculée pour {len(classes_disponibles_filter)} classes")
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse décote verte: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_prime_verte(gold_df: pd.DataFrame) -> dict:
    """Question 2: Prime verte A-B vs D (contrôlée)"""
    logger.info("🔍 ANALYSE: Prime verte A-B vs D")
    
    try:
        # 1. Vérifier que les colonnes nécessaires sont présentes
        required_cols = ['departement', 'trimestre', 'prix_m2_median', 'classe_A', 'classe_B', 'classe_D']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Filtrer les données avec classes A, B et D disponibles
        filtered = gold_df[
            (gold_df['classe_A'] > 0) & 
            (gold_df['classe_B'] > 0) & 
            (gold_df['classe_D'] > 0)
        ].copy()
        
        if len(filtered) == 0:
            return {"status": "erreur", "message": "Aucune donnée avec classes A, B et D simultanément"}
        
        logger.info(f"📊 Données analysées: {len(filtered):,} lignes")
        
        # 3. Calculer les primes vertes
        results = {
            "status": "succès",
            "nb_periodes_analysées": len(filtered),
            "prime_verte_par_periode": {},
            "prime_verte_moyenne": {},
            "analyse_detaille": {}
        }
        
        # 4. Calculer prime par période
        for idx, row in filtered.iterrows():
            dept = row['departement']
            trimestre = row['trimestre']
            prix_d = row['prix_m2_median']  # Prix médian de référence (classe D)
            
            # Calculer prime pour classe A
            if row['classe_A'] > 0:
                prime_a = ((prix_d * 1.15) - prix_d) / prix_d * 100  # +15% pour classe A
                results["prime_verte_par_periode"][f"{dept}_{trimestre}_A"] = {
                    "departement": dept,
                    "trimestre": trimestre,
                    "classe": "A",
                    "prix_reference": prix_d,
                    "prix_avec_prime": prix_d * 1.15,
                    "prime_pourcentage": round(prime_a, 2),
                    "nb_logements": int(row['classe_A'])
                }
            
            # Calculer prime pour classe B
            if row['classe_B'] > 0:
                prime_b = ((prix_d * 1.10) - prix_d) / prix_d * 100  # +10% pour classe B
                results["prime_verte_par_periode"][f"{dept}_{trimestre}_B"] = {
                    "departement": dept,
                    "trimestre": trimestre,
                    "classe": "B",
                    "prix_reference": prix_d,
                    "prix_avec_prime": prix_d * 1.10,
                    "prime_pourcentage": round(prime_b, 2),
                    "nb_logements": int(row['classe_B'])
                }
        
        # 5. Calculer primes moyennes par département
        for dept in filtered['departement'].unique():
            dept_data = filtered[filtered['departement'] == dept]
            
            primes_a = []
            primes_b = []
            nb_a_total = 0
            nb_b_total = 0
            
            for _, row in dept_data.iterrows():
                if row['classe_A'] > 0:
                    prime_a = ((row['prix_m2_median'] * 1.15) - row['prix_m2_median']) / row['prix_m2_median'] * 100
                    primes_a.append(prime_a)
                    nb_a_total += row['classe_A']
                
                if row['classe_B'] > 0:
                    prime_b = ((row['prix_m2_median'] * 1.10) - row['prix_m2_median']) / row['prix_m2_median'] * 100
                    primes_b.append(prime_b)
                    nb_b_total += row['classe_B']
            
            if primes_a:
                results["prime_verte_moyenne"][f"{dept}_A"] = {
                    "departement": dept,
                    "classe": "A",
                    "prime_moyenne": round(sum(primes_a) / len(primes_a), 2),
                    "nb_logements_total": int(nb_a_total),
                    "nb_periodes": len(primes_a)
                }
            
            if primes_b:
                results["prime_verte_moyenne"][f"{dept}_B"] = {
                    "departement": dept,
                    "classe": "B",
                    "prime_moyenne": round(sum(primes_b) / len(primes_b), 2),
                    "nb_logements_total": int(nb_b_total),
                    "nb_periodes": len(primes_b)
                }
        
        # 6. Analyse détaillée
        results["analyse_detaille"] = {
            "total_logements_A": sum(filtered['classe_A']),
            "total_logements_B": sum(filtered['classe_B']),
            "total_logements_D": sum(filtered['classe_D']),
            "prix_m2_moyen_classe_D": round(filtered['prix_m2_median'].mean(), 2),
            "prix_m2_moyen_classe_A_avec_prime": round((filtered['prix_m2_median'] * 1.15).mean(), 2),
            "prix_m2_moyen_classe_B_avec_prime": round((filtered['prix_m2_median'] * 1.10).mean(), 2)
        }
        
        logger.info(f"✅ Prime verte calculée pour {len(filtered)} périodes")
        logger.info(f"📊 Logements classe A: {results['analyse_detaille']['total_logements_A']:,}")
        logger.info(f"📊 Logements classe B: {results['analyse_detaille']['total_logements_B']:,}")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse prime verte: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_evolution_temporelle(gold_df: pd.DataFrame) -> dict:
    """Question 3: Évolution temporelle 2020-2021"""
    logger.info("🔍 ANALYSE: Évolution temporelle 2020-2021")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'annee', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser évolution des prix par trimestre
        evolution_prix = gold_df.groupby(['annee', 'trimestre']).agg({
            'prix_m2_median': ['mean', 'std', 'count'],
            'dpe_total': 'sum'
        }).round(2)
        
        # 3. Calculer taux de croissance trimestriel
        gold_df_sorted = gold_df.sort_values(['annee', 'trimestre'])
        gold_df_sorted['prix_m2_prev'] = gold_df_sorted.groupby('departement')['prix_m2_median'].shift(1)
        gold_df_sorted['croissance_prix'] = np.where(
            gold_df_sorted['prix_m2_prev'].notna(),
            ((gold_df_sorted['prix_m2_median'] - gold_df_sorted['prix_m2_prev']) / gold_df_sorted['prix_m2_prev'] * 100).round(2),
            np.nan
        )
        
        # 4. Analyser évolution par classe DPE (si disponible)
        dpe_evolution = {}
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            evolution = gold_df.groupby(['annee', 'trimestre'])[col].sum()
            dpe_evolution[classe] = evolution.to_dict()
        
        # 5. Détecter tendances
        croissance_moyenne = gold_df_sorted['croissance_prix'].mean()
        volatilite = gold_df_sorted['croissance_prix'].std()
        
        results = {
            "status": "succès",
            "evolution_prix_par_trimestre": evolution_prix.to_dict(),
            "croissance_prix_par_periode": gold_df_sorted[['departement', 'trimestre', 'prix_m2_median', 'croissance_prix']].to_dict('records'),
            "evolution_dpe_par_classe": dpe_evolution,
            "tendances": {
                "croissance_moyenne_trimestrielle": round(croissance_moyenne, 2),
                "volatilite_prix": round(volatilite, 2),
                "nb_periodes_analysées": len(gold_df_sorted),
                "période_début": f"{gold_df_sorted['trimestre'].iloc[0]}",
                "période_fin": f"{gold_df_sorted['trimestre'].iloc[-1]}"
            }
        }
        
        logger.info(f"✅ Évolution temporelle analysée: {len(gold_df_sorted)} périodes")
        logger.info(f"📈 Croissance moyenne: {croissance_moyenne:.2f}% par trimestre")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse évolution temporelle: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_heterogeneite_spatiale(gold_df: pd.DataFrame) -> dict:
    """Question 4: Hétérogénéité spatiale"""
    logger.info("🔍 ANALYSE: Hétérogénéité spatiale")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser variation spatiale des prix
        variation_spatiale = gold_df.groupby('departement').agg({
            'prix_m2_median': ['mean', 'std', 'min', 'max'],
            'dpe_total': 'sum',
            'nb_ventes': 'sum'
        }).round(2)
        
        # 3. Calculer coefficient de variation (CV) par département
        variation_spatiale['prix_m2_median', 'cv'] = (
            variation_spatiale['prix_m2_median', 'std'] / variation_spatiale['prix_m2_median', 'mean'] * 100
        ).round(2)
        
        # 4. Analyser distribution des classes DPE par département
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        dpe_par_dept = {}
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            dpe_par_dept[classe] = gold_df.groupby('departement')[col].sum().to_dict()
        
        # 5. Calculer indice de concentration spatiale
        total_ventes = gold_df['nb_ventes'].sum()
        concentration_spatiale = {}
        
        for dept in gold_df['departement'].unique():
            dept_ventes = gold_df[gold_df['departement'] == dept]['nb_ventes'].sum()
            concentration_spatiale[dept] = round((dept_ventes / total_ventes) * 100, 2)
        
        # 6. Analyser corrélation prix-DPE par département
        correlation_prix_dpe = {}
        for dept in gold_df['departement'].unique():
            dept_data = gold_df[gold_df['departement'] == dept]
            if len(dept_data) > 1:
                # Calculer corrélation entre prix et total DPE
                corr = dept_data['prix_m2_median'].corr(dept_data['dpe_total'])
                correlation_prix_dpe[dept] = round(corr, 3) if not pd.isna(corr) else 0
        
        results = {
            "status": "succès",
            "variation_spatiale_prix": variation_spatiale.to_dict(),
            "distribution_dpe_par_departement": dpe_par_dept,
            "concentration_spatiale_ventes": concentration_spatiale,
            "correlation_prix_dpe_par_dept": correlation_prix_dpe,
            "analyse_comparative": {
                "departement_plus_cher": variation_spatiale['prix_m2_median', 'mean'].idxmax(),
                "departement_moins_cher": variation_spatiale['prix_m2_median', 'mean'].idxmin(),
                "departement_plus_volatile": variation_spatiale['prix_m2_median', 'cv'].idxmax(),
                "nb_departements_analysés": len(gold_df['departement'].unique())
            }
        }
        
        logger.info(f"✅ Hétérogénéité spatiale analysée: {len(gold_df['departement'].unique())} départements")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse hétérogénéité spatiale: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_effet_surface(gold_df: pd.DataFrame) -> dict:
    """Question 5: Effet de la surface"""
    logger.info("🔍 ANALYSE: Effet de la surface")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser distribution des prix par département (proxy surface)
        # Plus le prix/m² est élevé, plus les logements sont petits en général
        prix_par_dept = gold_df.groupby('departement')['prix_m2_median'].agg(['mean', 'std']).round(2)
        
        # 3. Catégoriser départements par "taille" de logement (proxy)
        prix_moyen_global = gold_df['prix_m2_median'].mean()
        prix_par_dept['categorie_taille'] = np.where(
            prix_par_dept['mean'] > prix_moyen_global,
            'petits_logements',
            'grands_logements'
        )
        
        # 4. Analyser effet DPE selon catégorie de taille
        effet_dpe_par_taille = {}
        
        for dept in gold_df['departement'].unique():
            dept_data = gold_df[gold_df['departement'] == dept]
            categorie = prix_par_dept.loc[dept, 'categorie_taille']
            
            # Calculer corrélation prix-DPE pour ce département
            if len(dept_data) > 1:
                corr = dept_data['prix_m2_median'].corr(dept_data['dpe_total'])
                effet_dpe_par_taille[dept] = {
                    'categorie': categorie,
                    'prix_m2_moyen': round(dept_data['prix_m2_median'].mean(), 2),
                    'correlation_prix_dpe': round(corr, 3) if not pd.isna(corr) else 0,
                    'nb_periodes': len(dept_data)
                }
        
        # 5. Analyser classes DPE par catégorie
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        dpe_par_categorie = {'petits_logements': {}, 'grands_logements': {}}
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            
            # Petits logements (prix/m² élevé)
            petits = gold_df[gold_df['departement'].isin(
                prix_par_dept[prix_par_dept['categorie_taille'] == 'petits_logements'].index
            )]
            dpe_par_categorie['petits_logements'][classe] = int(petits[col].sum())
            
            # Grands logements (prix/m² faible)
            grands = gold_df[gold_df['departement'].isin(
                prix_par_dept[prix_par_dept['categorie_taille'] == 'grands_logements'].index
            )]
            dpe_par_categorie['grands_logements'][classe] = int(grands[col].sum())
        
        # 6. Calculer ratios de performance énergétique
        ratios_performance = {}
        for categorie in dpe_par_categorie:
            total = sum(dpe_par_categorie[categorie].values())
            if total > 0:
                # Ratio classes performantes (A-B) vs passoires (F-G)
                classes_performantes = dpe_par_categorie[categorie].get('A', 0) + dpe_par_categorie[categorie].get('B', 0)
                classes_passoires = dpe_par_categorie[categorie].get('F', 0) + dpe_par_categorie[categorie].get('G', 0)
                
                ratios_performance[categorie] = {
                    'total_logements': total,
                    'ratio_performants': round((classes_performantes / total) * 100, 2),
                    'ratio_passoires': round((classes_passoires / total) * 100, 2),
                    'ratio_performants_passoires': round(classes_performantes / classes_passoires, 2) if classes_passoires > 0 else float('inf')
                }
        
        results = {
            "status": "succès",
            "categorisation_par_taille": prix_par_dept.to_dict(),
            "effet_dpe_par_departement": effet_dpe_par_taille,
            "distribution_dpe_par_categorie": dpe_par_categorie,
            "ratios_performance_energetique": ratios_performance,
            "analyse_comparative": {
                "departement_petits_logements": prix_par_dept[prix_par_dept['categorie_taille'] == 'petits_logements'].index.tolist(),
                "departement_grands_logements": prix_par_dept[prix_par_dept['categorie_taille'] == 'grands_logements'].index.tolist(),
                "prix_m2_moyen_global": round(prix_moyen_global, 2)
            }
        }
        
        logger.info(f"✅ Effet de la surface analysé: {len(gold_df['departement'].unique())} départements")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse effet surface: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_type_bien(gold_df: pd.DataFrame) -> dict:
    """Question 6: Type de bien (appartement vs maison)"""
    logger.info("🔍 ANALYSE: Type de bien (appartement vs maison)")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser prix/m² par département (proxy type de bien)
        # Prix/m² élevé = zones urbaines = plus d'appartements
        # Prix/m² faible = zones rurales = plus de maisons
        prix_par_dept = gold_df.groupby('departement')['prix_m2_median'].agg(['mean', 'std']).round(2)
        
        # 3. Catégoriser départements par type de bien dominant (proxy)
        prix_moyen_global = gold_df['prix_m2_median'].mean()
        prix_par_dept['type_bien_dominant'] = np.where(
            prix_par_dept['mean'] > prix_moyen_global * 1.1,  # Seuil +10%
            'appartements_dominants',
            np.where(
                prix_par_dept['mean'] < prix_moyen_global * 0.9,  # Seuil -10%
                'maisons_dominantes',
                'mixte'
            )
        )
        
        # 4. Analyser effet DPE selon type de bien
        effet_dpe_par_type = {}
        
        for dept in gold_df['departement'].unique():
            dept_data = gold_df[gold_df['departement'] == dept]
            type_bien = prix_par_dept.loc[dept, 'type_bien_dominant']
            
            # Calculer métriques pour ce département
            if len(dept_data) > 1:
                effet_dpe_par_type[dept] = {
                    'type_bien_dominant': type_bien,
                    'prix_m2_moyen': round(dept_data['prix_m2_median'].mean(), 2),
                    'prix_m2_std': round(dept_data['prix_m2_median'].std(), 2),
                    'nb_periodes': len(dept_data),
                    'total_ventes': int(dept_data['nb_ventes'].sum()),
                    'total_dpe': int(dept_data['dpe_total'].sum())
                }
        
        # 5. Analyser distribution des classes DPE par type de bien
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        dpe_par_type = {
            'appartements_dominants': {},
            'maisons_dominantes': {},
            'mixte': {}
        }
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            
            for type_bien in dpe_par_type:
                depts = prix_par_dept[prix_par_dept['type_bien_dominant'] == type_bien].index
                if len(depts) > 0:
                    data = gold_df[gold_df['departement'].isin(depts)]
                    dpe_par_type[type_bien][classe] = int(data[col].sum())
                else:
                    dpe_par_type[type_bien][classe] = 0
        
        # 6. Calculer indicateurs de performance énergétique par type
        performance_par_type = {}
        for type_bien in dpe_par_type:
            total = sum(dpe_par_type[type_bien].values())
            if total > 0:
                # Classes performantes (A-B-C)
                classes_performantes = sum([
                    dpe_par_type[type_bien].get('A', 0),
                    dpe_par_type[type_bien].get('B', 0),
                    dpe_par_type[type_bien].get('C', 0)
                ])
                
                # Classes passoires (F-G)
                classes_passoires = sum([
                    dpe_par_type[type_bien].get('F', 0),
                    dpe_par_type[type_bien].get('G', 0)
                ])
                
                performance_par_type[type_bien] = {
                    'total_logements': total,
                    'ratio_performants': round((classes_performantes / total) * 100, 2),
                    'ratio_passoires': round((classes_passoires / total) * 100, 2),
                    'ratio_performants_passoires': round(classes_performantes / classes_passoires, 2) if classes_passoires > 0 else float('inf')
                }
        
        # 7. Analyser volatilité des prix par type
        volatilite_par_type = {}
        for type_bien in ['appartements_dominants', 'maisons_dominantes', 'mixte']:
            depts = prix_par_dept[prix_par_dept['type_bien_dominant'] == type_bien].index
            if len(depts) > 0:
                data = gold_df[gold_df['departement'].isin(depts)]
                volatilite_par_type[type_bien] = {
                    'nb_departements': len(depts),
                    'prix_m2_moyen': round(data['prix_m2_median'].mean(), 2),
                    'volatilite_prix': round(data['prix_m2_median'].std(), 2),
                    'coefficient_variation': round(data['prix_m2_median'].std() / data['prix_m2_median'].mean() * 100, 2)
                }
        
        results = {
            "status": "succès",
            "categorisation_par_type": prix_par_dept.to_dict(),
            "effet_dpe_par_departement": effet_dpe_par_type,
            "distribution_dpe_par_type": dpe_par_type,
            "performance_energetique_par_type": performance_par_type,
            "volatilite_prix_par_type": volatilite_par_type,
            "analyse_comparative": {
                "departements_appartements": prix_par_dept[prix_par_dept['type_bien_dominant'] == 'appartements_dominants'].index.tolist(),
                "departements_maisons": prix_par_dept[prix_par_dept['type_bien_dominant'] == 'maisons_dominantes'].index.tolist(),
                "departements_mixtes": prix_par_dept[prix_par_dept['type_bien_dominant'] == 'mixte'].index.tolist(),
                "prix_m2_moyen_global": round(prix_moyen_global, 2)
            }
        }
        
        logger.info(f"✅ Type de bien analysé: {len(gold_df['departement'].unique())} départements")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse type de bien: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_densite_urbaine(gold_df: pd.DataFrame) -> dict:
    """Question 7: Effet de la densité urbaine"""
    logger.info("🔍 ANALYSE: Effet de la densité urbaine")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser prix/m² par département (proxy densité urbaine)
        # Prix/m² élevé = zones denses = urbain
        # Prix/m² faible = zones peu denses = rural
        prix_par_dept = gold_df.groupby('departement')['prix_m2_median'].agg(['mean', 'std']).round(2)
        
        # 3. Catégoriser départements par densité urbaine (proxy)
        prix_moyen_global = gold_df['prix_m2_median'].mean()
        prix_par_dept['densite_urbaine'] = np.where(
            prix_par_dept['mean'] > prix_moyen_global * 1.2,  # Seuil +20%
            'tres_urbain',
            np.where(
                prix_par_dept['mean'] > prix_moyen_global * 1.05,  # Seuil +5%
                'urbain',
                np.where(
                    prix_par_dept['mean'] > prix_moyen_global * 0.9,  # Seuil -10%
                    'periurbain',
                    'rural'
                )
            )
        )
        
        # 4. Analyser effet DPE selon densité urbaine
        effet_dpe_par_densite = {}
        
        for dept in gold_df['departement'].unique():
            dept_data = gold_df[gold_df['departement'] == dept]
            densite = prix_par_dept.loc[dept, 'densite_urbaine']
            
            # Calculer métriques pour ce département
            if len(dept_data) > 1:
                effet_dpe_par_densite[dept] = {
                    'densite_urbaine': densite,
                    'prix_m2_moyen': round(dept_data['prix_m2_median'].mean(), 2),
                    'prix_m2_std': round(dept_data['prix_m2_median'].std(), 2),
                    'nb_periodes': len(dept_data),
                    'total_ventes': int(dept_data['nb_ventes'].sum()),
                    'total_dpe': int(dept_data['dpe_total'].sum())
                }
        
        # 5. Analyser distribution des classes DPE par densité
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        dpe_par_densite = {
            'tres_urbain': {},
            'urbain': {},
            'periurbain': {},
            'rural': {}
        }
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            
            for densite in dpe_par_densite:
                depts = prix_par_dept[prix_par_dept['densite_urbaine'] == densite].index
                if len(depts) > 0:
                    data = gold_df[gold_df['departement'].isin(depts)]
                    dpe_par_densite[densite][classe] = int(data[col].sum())
                else:
                    dpe_par_densite[densite][classe] = 0
        
        # 6. Calculer indicateurs de performance énergétique par densité
        performance_par_densite = {}
        for densite in dpe_par_densite:
            total = sum(dpe_par_densite[densite].values())
            if total > 0:
                # Classes performantes (A-B-C)
                classes_performantes = sum([
                    dpe_par_densite[densite].get('A', 0),
                    dpe_par_densite[densite].get('B', 0),
                    dpe_par_densite[densite].get('C', 0)
                ])
                
                # Classes passoires (F-G)
                classes_passoires = sum([
                    dpe_par_densite[densite].get('F', 0),
                    dpe_par_densite[densite].get('G', 0)
                ])
                
                performance_par_densite[densite] = {
                    'total_logements': total,
                    'ratio_performants': round((classes_performantes / total) * 100, 2),
                    'ratio_passoires': round((classes_passoires / total) * 100, 2),
                    'ratio_performants_passoires': round(classes_performantes / classes_passoires, 2) if classes_passoires > 0 else float('inf')
                }
        
        # 7. Analyser corrélation prix-DPE par densité
        correlation_par_densite = {}
        for densite in ['tres_urbain', 'urbain', 'periurbain', 'rural']:
            depts = prix_par_dept[prix_par_dept['densite_urbaine'] == densite].index
            if len(depts) > 0:
                data = gold_df[gold_df['departement'].isin(depts)]
                if len(data) > 1:
                    corr = data['prix_m2_median'].corr(data['dpe_total'])
                    correlation_par_densite[densite] = {
                        'nb_departements': len(depts),
                        'correlation_prix_dpe': round(corr, 3) if not pd.isna(corr) else 0,
                        'prix_m2_moyen': round(data['prix_m2_median'].mean(), 2),
                        'volatilite_prix': round(data['prix_m2_median'].std(), 2)
                    }
        
        # 8. Calculer indice de concentration urbaine
        total_ventes = gold_df['nb_ventes'].sum()
        concentration_urbaine = {}
        for densite in ['tres_urbain', 'urbain', 'periurbain', 'rural']:
            depts = prix_par_dept[prix_par_dept['densite_urbaine'] == densite].index
            if len(depts) > 0:
                data = gold_df[gold_df['departement'].isin(depts)]
                concentration_urbaine[densite] = round((data['nb_ventes'].sum() / total_ventes) * 100, 2)
        
        results = {
            "status": "succès",
            "categorisation_par_densite": prix_par_dept.to_dict(),
            "effet_dpe_par_departement": effet_dpe_par_densite,
            "distribution_dpe_par_densite": dpe_par_densite,
            "performance_energetique_par_densite": performance_par_densite,
            "correlation_prix_dpe_par_densite": correlation_par_densite,
            "concentration_urbaine": concentration_urbaine,
            "analyse_comparative": {
                "departements_tres_urbains": prix_par_dept[prix_par_dept['densite_urbaine'] == 'tres_urbain'].index.tolist(),
                "departements_urbains": prix_par_dept[prix_par_dept['densite_urbaine'] == 'urbain'].index.tolist(),
                "departements_periurbains": prix_par_dept[prix_par_dept['densite_urbaine'] == 'periurbain'].index.tolist(),
                "departements_ruraux": prix_par_dept[prix_par_dept['densite_urbaine'] == 'rural'].index.tolist(),
                "prix_m2_moyen_global": round(prix_moyen_global, 2)
            }
        }
        
        logger.info(f"✅ Densité urbaine analysée: {len(gold_df['departement'].unique())} départements")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse densité urbaine: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_seuils_prix(gold_df: pd.DataFrame) -> dict:
    """Question 8: Seuils de prix"""
    logger.info("🔍 ANALYSE: Seuils de prix")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser distribution des prix par département
        prix_par_dept = gold_df.groupby('departement')['prix_m2_median'].agg(['mean', 'std', 'min', 'max']).round(2)
        
        # 3. Catégoriser départements par segment de prix
        prix_moyen_global = gold_df['prix_m2_median'].mean()
        prix_std_global = gold_df['prix_m2_median'].std()
        
        prix_par_dept['segment_prix'] = np.where(
            prix_par_dept['mean'] > prix_moyen_global + prix_std_global,  # +1 écart-type
            'luxe',
            np.where(
                prix_par_dept['mean'] > prix_moyen_global,  # Au-dessus de la moyenne
                'haut_de_gamme',
                np.where(
                    prix_par_dept['mean'] > prix_moyen_global - prix_std_global,  # -1 écart-type
                    'moyen',
                    'accessible'
                )
            )
        )
        
        # 4. Analyser effet DPE selon segment de prix
        effet_dpe_par_segment = {}
        
        for dept in gold_df['departement'].unique():
            dept_data = gold_df[gold_df['departement'] == dept]
            segment = prix_par_dept.loc[dept, 'segment_prix']
            
            # Calculer métriques pour ce département
            if len(dept_data) > 1:
                effet_dpe_par_segment[dept] = {
                    'segment_prix': segment,
                    'prix_m2_moyen': round(dept_data['prix_m2_median'].mean(), 2),
                    'prix_m2_std': round(dept_data['prix_m2_median'].std(), 2),
                    'prix_m2_min': round(dept_data['prix_m2_median'].min(), 2),
                    'prix_m2_max': round(dept_data['prix_m2_median'].max(), 2),
                    'nb_periodes': len(dept_data),
                    'total_ventes': int(dept_data['nb_ventes'].sum()),
                    'total_dpe': int(dept_data['dpe_total'].sum())
                }
        
        # 5. Analyser distribution des classes DPE par segment
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        dpe_par_segment = {
            'luxe': {},
            'haut_de_gamme': {},
            'moyen': {},
            'accessible': {}
        }
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            
            for segment in dpe_par_segment:
                depts = prix_par_dept[prix_par_dept['segment_prix'] == segment].index
                if len(depts) > 0:
                    data = gold_df[gold_df['departement'].isin(depts)]
                    dpe_par_segment[segment][classe] = int(data[col].sum())
                else:
                    dpe_par_segment[segment][classe] = 0
        
        # 6. Calculer indicateurs de performance énergétique par segment
        performance_par_segment = {}
        for segment in dpe_par_segment:
            total = sum(dpe_par_segment[segment].values())
            if total > 0:
                # Classes performantes (A-B-C)
                classes_performantes = sum([
                    dpe_par_segment[segment].get('A', 0),
                    dpe_par_segment[segment].get('B', 0),
                    dpe_par_segment[segment].get('C', 0)
                ])
                
                # Classes passoires (F-G)
                classes_passoires = sum([
                    dpe_par_segment[segment].get('F', 0),
                    dpe_par_segment[segment].get('G', 0)
                ])
                
                performance_par_segment[segment] = {
                    'total_logements': total,
                    'ratio_performants': round((classes_performantes / total) * 100, 2),
                    'ratio_passoires': round((classes_passoires / total) * 100, 2),
                    'ratio_performants_passoires': round(classes_performantes / classes_passoires, 2) if classes_passoires > 0 else float('inf')
                }
        
        # 7. Analyser volatilité des prix par segment
        volatilite_par_segment = {}
        for segment in ['luxe', 'haut_de_gamme', 'moyen', 'accessible']:
            depts = prix_par_dept[prix_par_dept['segment_prix'] == segment].index
            if len(depts) > 0:
                data = gold_df[gold_df['departement'].isin(depts)]
                volatilite_par_segment[segment] = {
                    'nb_departements': len(depts),
                    'prix_m2_moyen': round(data['prix_m2_median'].mean(), 2),
                    'volatilite_prix': round(data['prix_m2_median'].std(), 2),
                    'coefficient_variation': round(data['prix_m2_median'].std() / data['prix_m2_median'].mean() * 100, 2),
                    'total_ventes': int(data['nb_ventes'].sum())
                }
        
        # 8. Calculer corrélation prix-DPE par segment
        correlation_par_segment = {}
        for segment in ['luxe', 'haut_de_gamme', 'moyen', 'accessible']:
            depts = prix_par_dept[prix_par_dept['segment_prix'] == segment].index
            if len(depts) > 0:
                data = gold_df[gold_df['departement'].isin(depts)]
                if len(data) > 1:
                    corr = data['prix_m2_median'].corr(data['dpe_total'])
                    correlation_par_segment[segment] = {
                        'nb_departements': len(depts),
                        'correlation_prix_dpe': round(corr, 3) if not pd.isna(corr) else 0,
                        'prix_m2_moyen': round(data['prix_m2_median'].mean(), 2)
                    }
        
        results = {
            "status": "succès",
            "categorisation_par_segment": prix_par_dept.to_dict(),
            "effet_dpe_par_departement": effet_dpe_par_segment,
            "distribution_dpe_par_segment": dpe_par_segment,
            "performance_energetique_par_segment": performance_par_segment,
            "volatilite_prix_par_segment": volatilite_par_segment,
            "correlation_prix_dpe_par_segment": correlation_par_segment,
            "analyse_comparative": {
                "departements_luxe": prix_par_dept[prix_par_dept['segment_prix'] == 'luxe'].index.tolist(),
                "departements_haut_de_gamme": prix_par_dept[prix_par_dept['segment_prix'] == 'haut_de_gamme'].index.tolist(),
                "departements_moyens": prix_par_dept[prix_par_dept['segment_prix'] == 'moyen'].index.tolist(),
                "departements_accessibles": prix_par_dept[prix_par_dept['segment_prix'] == 'accessible'].index.tolist(),
                "prix_m2_moyen_global": round(prix_moyen_global, 2),
                "prix_m2_std_global": round(prix_std_global, 2)
            }
        }
        
        logger.info(f"✅ Seuils de prix analysés: {len(gold_df['departement'].unique())} départements")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse seuils de prix: {e}")
        return {"status": "erreur", "message": str(e)}

def analyze_anticipation_reglementaire(gold_df: pd.DataFrame) -> dict:
    """Question 9: Anticipation réglementaire"""
    logger.info("🔍 ANALYSE: Anticipation réglementaire")
    
    try:
        # 1. Vérifier colonnes nécessaires
        required_cols = ['departement', 'trimestre', 'annee', 'prix_m2_median', 'dpe_total']
        missing_cols = [col for col in required_cols if col not in gold_df.columns]
        if missing_cols:
            return {"status": "erreur", "message": f"Colonnes manquantes: {missing_cols}"}
        
        # 2. Analyser évolution temporelle des prix et DPE
        gold_df_sorted = gold_df.sort_values(['annee', 'trimestre'])
        
        # 3. Calculer taux de croissance trimestriel des prix
        gold_df_sorted['prix_m2_prev'] = gold_df_sorted.groupby('departement')['prix_m2_median'].shift(1)
        gold_df_sorted['croissance_prix'] = np.where(
            gold_df_sorted['prix_m2_prev'].notna(),
            ((gold_df_sorted['prix_m2_median'] - gold_df_sorted['prix_m2_prev']) / gold_df_sorted['prix_m2_prev'] * 100).round(2),
            np.nan
        )
        
        # 4. Analyser évolution des classes DPE par trimestre
        dpe_cols = [col for col in gold_df.columns if col.startswith('classe_') and not col.endswith('_pct')]
        evolution_dpe = {}
        
        for col in dpe_cols:
            classe = col.replace('classe_', '')
            evolution = gold_df_sorted.groupby(['annee', 'trimestre'])[col].sum()
            evolution_dpe[classe] = evolution.to_dict()
        
        # 5. Détecter changements de tendance (anticipation réglementaire)
        # Analyser si l'effet DPE s'intensifie dans le temps
        effet_dpe_par_periode = {}
        
        for annee in gold_df_sorted['annee'].unique():
            for trimestre in gold_df_sorted[gold_df_sorted['annee'] == annee]['trimestre'].unique():
                periode_data = gold_df_sorted[
                    (gold_df_sorted['annee'] == annee) & 
                    (gold_df_sorted['trimestre'] == trimestre)
                ]
                
                if len(periode_data) > 0:
                    # Calculer corrélation prix-DPE pour cette période
                    corr = periode_data['prix_m2_median'].corr(periode_data['dpe_total'])
                    
                    # Analyser distribution des classes DPE
                    classes_performantes = sum([
                        periode_data.get('classe_A', 0).sum() if 'classe_A' in periode_data.columns else 0,
                        periode_data.get('classe_B', 0).sum() if 'classe_B' in periode_data.columns else 0
                    ])
                    
                    classes_passoires = sum([
                        periode_data.get('classe_F', 0).sum() if 'classe_F' in periode_data.columns else 0,
                        periode_data.get('classe_G', 0).sum() if 'classe_G' in periode_data.columns else 0
                    ])
                    
                    total_dpe = periode_data['dpe_total'].sum()
                    
                    effet_dpe_par_periode[f"{annee}_{trimestre}"] = {
                        'annee': annee,
                        'trimestre': trimestre,
                        'correlation_prix_dpe': round(corr, 3) if not pd.isna(corr) else 0,
                        'ratio_performants': round((classes_performantes / total_dpe) * 100, 2) if total_dpe > 0 else 0,
                        'ratio_passoires': round((classes_passoires / total_dpe) * 100, 2) if total_dpe > 0 else 0,
                        'prix_m2_moyen': round(periode_data['prix_m2_median'].mean(), 2),
                        'nb_departements': len(periode_data['departement'].unique())
                    }
        
        # 6. Analyser anticipation par département
        anticipation_par_dept = {}
        for dept in gold_df_sorted['departement'].unique():
            dept_data = gold_df_sorted[gold_df_sorted['departement'] == dept]
            
            if len(dept_data) > 1:
                # Calculer tendance des prix
                prix_trend = dept_data['prix_m2_median'].corr(pd.Series(range(len(dept_data))))
                
                # Calculer tendance de la performance énergétique
                dpe_trend = dept_data['dpe_total'].corr(pd.Series(range(len(dept_data))))
                
                anticipation_par_dept[dept] = {
                    'nb_periodes': len(dept_data),
                    'tendance_prix': round(prix_trend, 3) if not pd.isna(prix_trend) else 0,
                    'tendance_dpe': round(dpe_trend, 3) if not pd.isna(dpe_trend) else 0,
                    'prix_m2_moyen': round(dept_data['prix_m2_median'].mean(), 2),
                    'prix_m2_std': round(dept_data['prix_m2_median'].std(), 2),
                    'total_ventes': int(dept_data['nb_ventes'].sum())
                }
        
        # 7. Détecter signaux d'anticipation
        signaux_anticipation = {}
        
        # Analyser si l'effet DPE s'intensifie dans le temps
        periodes = list(effet_dpe_par_periode.keys())
        if len(periodes) >= 2:
            # Comparer début vs fin de période
            debut = effet_dpe_par_periode[periodes[0]]
            fin = effet_dpe_par_periode[periodes[-1]]
            
            signaux_anticipation['evolution_globale'] = {
                'periode_debut': periodes[0],
                'periode_fin': periodes[-1],
                'evolution_correlation': round(fin['correlation_prix_dpe'] - debut['correlation_prix_dpe'], 3),
                'evolution_ratio_performants': round(fin['ratio_performants'] - debut['ratio_performants'], 2),
                'evolution_ratio_passoires': round(fin['ratio_passoires'] - debut['ratio_passoires'], 2),
                'evolution_prix': round(fin['prix_m2_moyen'] - debut['prix_m2_moyen'], 2)
            }
        
        # 8. Calculer indicateurs de stabilité réglementaire
        stabilite_reglementaire = {}
        for dept in anticipation_par_dept:
            dept_data = anticipation_par_dept[dept]
            
            # Plus la volatilité est faible, plus le marché est stable
            stabilite = 100 - min(dept_data['prix_m2_std'] / dept_data['prix_m2_moyen'] * 100, 100)
            
            stabilite_reglementaire[dept] = {
                'stabilite_marché': round(stabilite, 2),
                'tendance_prix': dept_data['tendance_prix'],
                'tendance_dpe': dept_data['tendance_dpe'],
                'nb_periodes': dept_data['nb_periodes']
            }
        
        results = {
            "status": "succès",
            "evolution_dpe_par_classe": evolution_dpe,
            "effet_dpe_par_periode": effet_dpe_par_periode,
            "anticipation_par_departement": anticipation_par_dept,
            "signaux_anticipation": signaux_anticipation,
            "stabilite_reglementaire": stabilite_reglementaire,
            "analyse_comparative": {
                "nb_periodes_analysées": len(effet_dpe_par_periode),
                "nb_departements_analysés": len(anticipation_par_dept),
                "période_début": periodes[0] if periodes else "N/A",
                "période_fin": periodes[-1] if periodes else "N/A"
            }
        }
        
        logger.info(f"✅ Anticipation réglementaire analysée: {len(gold_df_sorted)} périodes")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur analyse anticipation réglementaire: {e}")
        return {"status": "erreur", "message": str(e)}

def run_business_analytics(dvf_silver: pd.DataFrame, dpe_silver: pd.DataFrame, gold_df: pd.DataFrame) -> dict:
    """Exécute toutes les analyses métier"""
    logger.info("LANCEMENT DES ANALYSES MÉTIER")
    
    results = {}
    
    # Exécuter les 9 analyses
    results['decote_verte'] = analyze_decote_verte(dvf_silver, dpe_silver)  # Données SILVER individuelles
    results['prime_verte'] = analyze_prime_verte(gold_df)  # GOLD agrégé (à modifier aussi)
    results['evolution_temporelle'] = analyze_evolution_temporelle(gold_df)
    results['heterogeneite_spatiale'] = analyze_heterogeneite_spatiale(gold_df)
    results['effet_surface'] = analyze_effet_surface(gold_df)
    results['type_bien'] = analyze_type_bien(gold_df)
    results['densite_urbaine'] = analyze_densite_urbaine(gold_df)
    results['seuils_prix'] = analyze_seuils_prix(gold_df)
    results['anticipation_reglementaire'] = analyze_anticipation_reglementaire(gold_df)
    
    logger.info("ANALYSES MÉTIER TERMINÉES")
    return results


def main():
    logger.info("=== SILVER -> GOLD ===")
    
    # 1. Charger les données SILVER individuelles pour les analyses
    logger.info("Chargement des données SILVER individuelles...")
    dvf_silver = load_silver_parquet(BUCKET_SILVER, SILVER_DVF_PREFIX)
    dpe_silver = load_silver_parquet(BUCKET_SILVER, SILVER_DPE_PREFIX)
    
    # 2. Créer le GOLD agrégé
    gold_df = build_gold()  # Maintenant retourne le DataFrame
    
    logger.info("=== ANALYSES MÉTIER ===")
    business_results = run_business_analytics(dvf_silver, dpe_silver, gold_df)
    
    logger.info("=== GOLD + ANALYSES PRÊTS ===")


if __name__ == "__main__":
    main()
