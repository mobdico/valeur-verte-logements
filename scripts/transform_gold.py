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
        logger.info(f"Bucket {name} présent ✅")
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket", "NotFound"):
            logger.info(f"Bucket {name} absent → création…")
            s3.create_bucket(Bucket=name)
            logger.info(f"Bucket {name} créé ✅")
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
    pq.write_to_dataset(
        table,
        root_path=root,
        filesystem=fs,
        partition_cols=partition_cols,
        existing_data_behavior="overwrite_or_ignore",
        compression="snappy",
        use_dictionary=True,
    )
    logger.info("Écriture GOLD OK ✅")


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

    # 4) Ajouter ‘annee’ pour compat partition
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


def main():
    logger.info("=== SILVER -> GOLD ===")
    build_gold()
    logger.info("=== GOLD prêt ===")


if __name__ == "__main__":
    main()
