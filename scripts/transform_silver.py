#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de transformation BRONZE -> SILVER
Nettoyage, typage et partitionnement des données DVF et DPE

PATCH 2025-08 :
- DVF : ajout d'une colonne 'code_departement' et partitionnement sur celle-ci
        (évite les partitions "Code departement=…" avec espace qui cassent la lecture).
"""

import logging
import boto3
import botocore
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# MinIO
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password123"

BRONZE_BUCKET = "datalake-bronze"
SILVER_BUCKET = "datalake-silver"

# Périmètre
DEPARTEMENTS = ["92", "59", "34"]

# Colonnes DVF utiles
DVF_COLS = [
    "Date mutation",
    "Valeur fonciere",
    "Code departement",
    "Code commune",
    "Type local",
    "Surface reelle bati",
]

# Colonnes DPE utiles
DPE_COLS = [
    "date_etablissement_dpe",
    "code_insee_commune_actualise",
    "classe_consommation_energie",
    "classe_estimation_ges",
    "tr002_type_batiment_description",
    "tv016_departement_code",
]

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def to_clean_str(x):
    """Convertit de façon robuste en string:
    - NaN/None -> None
    - float/int -> sans .0 (ex: 75056.0 -> '75056')
    - autre -> str(x).strip()
    """
    if pd.isna(x):
        return None
    if isinstance(x, float):
        if x.is_integer():
            return str(int(x))
        return str(x)
    if isinstance(x, (int,)):
        return str(x)
    s = str(x).strip()
    if s.endswith(".0") and s.replace(".", "", 1).isdigit():
        s = s[:-2]
    return s or None


# ---------------------------------------------------------------------
# Helpers S3/MinIO
# ---------------------------------------------------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=botocore.client.Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(bucket: str):
    """Crée le bucket s'il n'existe pas (idempotent)."""
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"Bucket {bucket} présent ✅")
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket", "NotFound"):
            logger.info(f"Bucket {bucket} absent → création…")
            s3.create_bucket(Bucket=bucket)
            logger.info(f"Bucket {bucket} créé ✅")
        else:
            raise


def get_pa_s3_filesystem() -> pafs.S3FileSystem:
    """PyArrow S3 filesystem pointant sur MinIO (sans SSL)."""
    return pafs.S3FileSystem(
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        scheme="http",
    )


def write_parquet_partitioned(
    table: pa.Table,
    bucket: str,
    prefix: str,
    partition_cols: list[str],
):
    """
    Écrit un dataset Parquet partitionné sur MinIO.
    IMPORTANT: root_path doit être 'bucket/prefix' (sans 's3://').
    """
    ensure_bucket(bucket)
    pa_fs = get_pa_s3_filesystem()
    base_dir = f"{bucket}/{prefix}".rstrip("/")

    logger.info(
        f"Écriture Parquet partitionné → {base_dir} (partitions={partition_cols})"
    )
    pq.write_to_dataset(
        table=table,
        root_path=base_dir,
        partition_cols=partition_cols,
        filesystem=pa_fs,
        existing_data_behavior="overwrite_or_ignore",
        use_dictionary=True,
        compression="snappy",
    )
    logger.info("Écriture Parquet OK ✅")


# ---------------------------------------------------------------------
# DVF → Silver
# ---------------------------------------------------------------------
def load_dvf_year_to_df(year: int) -> pd.DataFrame:
    """Charge un fichier DVF BRONZE depuis MinIO et filtre/typera les colonnes utiles."""
    s3 = get_s3_client()
    prefix = f"dvf/{year}/"
    logger.info(f"Lecture DVF BRONZE: s3://{BRONZE_BUCKET}/{prefix}")

    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    if "Contents" not in resp or not resp["Contents"]:
        logger.warning(f"Aucun fichier DVF trouvé pour {year}")
        return pd.DataFrame(columns=DVF_COLS)

    # On prend le premier fichier (pattern d’upload = un fichier par année)
    file_key = resp["Contents"][0]["Key"]

    df = pd.read_csv(
        f"s3://{BRONZE_BUCKET}/{file_key}",
        sep="|",
        usecols=lambda c: c in DVF_COLS,
        storage_options={
            "key": S3_ACCESS_KEY,
            "secret": S3_SECRET_KEY,
            "client_kwargs": {"endpoint_url": S3_ENDPOINT},
        },
        dtype=str,
        low_memory=False,
    )
    logger.info(f"DVF brut {year}: {len(df)} lignes")

    # Nettoyage minimal + features
    df = df.dropna(subset=["Valeur fonciere", "Surface reelle bati"])
    df["Valeur fonciere"] = df["Valeur fonciere"].str.replace(",", ".").astype(float)
    df["Surface reelle bati"] = df["Surface reelle bati"].str.replace(",", ".").astype(float)

    # Eviter division par 0
    df = df[df["Surface reelle bati"] > 0]
    df["prix_m2"] = df["Valeur fonciere"] / df["Surface reelle bati"]

    df["date_mutation"] = pd.to_datetime(df["Date mutation"], format="%d/%m/%Y", errors="coerce")
    df = df.dropna(subset=["date_mutation"])
    df["annee"] = df["date_mutation"].dt.year
    df["trimestre"] = df["date_mutation"].dt.to_period("Q").astype(str)

    # Filtre périmètre
    df = df[df["Code departement"].isin(DEPARTEMENTS)]

    logger.info(f"DVF filtré {year}: {len(df)} lignes")
    return df


def transform_dvf():
    frames = [load_dvf_year_to_df(2020), load_dvf_year_to_df(2021)]
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        logger.warning("DVF: dataset vide, abandon écriture Silver")
        return

    # 🔧 colonne de partition SANS espace
    df["code_departement"] = df["Code departement"].astype(str).str.strip()

    table = pa.Table.from_pandas(df, preserve_index=False)
    write_parquet_partitioned(
        table=table,
        bucket=SILVER_BUCKET,
        prefix="dvf",
        partition_cols=["code_departement", "annee", "trimestre"],
    )


# ---------------------------------------------------------------------
# DPE → Silver
# ---------------------------------------------------------------------
def transform_dpe():
    s3 = get_s3_client()
    frames = []

    for dept in DEPARTEMENTS:
        prefix = f"dpe/{dept}/"
        resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
        if "Contents" not in resp or not resp["Contents"]:
            logger.warning(f"Aucun lot DPE trouvé pour dept={dept}")
            continue

        for obj in resp["Contents"]:
            key = obj["Key"]
            logger.info(f"Lecture DPE BRONZE: s3://{BRONZE_BUCKET}/{key}")

            df = pd.read_json(
                f"s3://{BRONZE_BUCKET}/{key}",
                lines=False,
                storage_options={
                    "key": S3_ACCESS_KEY,
                    "secret": S3_SECRET_KEY,
                    "client_kwargs": {"endpoint_url": S3_ENDPOINT},
                },
            )

            # Garder seulement les colonnes utiles (silencieusement si manquantes)
            keep = [c for c in DPE_COLS if c in df.columns]
            missing = [c for c in DPE_COLS if c not in df.columns]
            if missing:
                logger.warning(f"Colonnes DPE manquantes dans {key}: {missing}")
            if not keep:
                logger.warning(f"Aucune colonne utile dans {key}, on saute.")
                continue

            df = df[keep]

            # Filtre périmètre
            if "tv016_departement_code" in df.columns:
                df["tv016_departement_code"] = df["tv016_departement_code"].map(to_clean_str)
                df = df[df["tv016_departement_code"].isin(DEPARTEMENTS)]

            # Dates / partitions
            if "date_etablissement_dpe" in df.columns:
                df["date_etablissement_dpe"] = pd.to_datetime(
                    df["date_etablissement_dpe"], errors="coerce"
                )
                df = df.dropna(subset=["date_etablissement_dpe"])
                df["annee"] = df["date_etablissement_dpe"].dt.year
                df["trimestre"] = df["date_etablissement_dpe"].dt.to_period("Q").astype(str)

            # Normalisation des colonnes textuelles -> string propre
            for col in [
                "code_insee_commune_actualise",
                "classe_consommation_energie",
                "classe_estimation_ges",
                "tr002_type_batiment_description",
            ]:
                if col in df.columns:
                    df[col] = df[col].map(to_clean_str)

            frames.append(df)

    if not frames:
        logger.warning("DPE: aucun fichier exploitable, abandon écriture Silver")
        return

    df_all = pd.concat(frames, ignore_index=True)

    # Sécurise les types avant Arrow
    if "annee" in df_all.columns:
        df_all["annee"] = pd.to_numeric(df_all["annee"], errors="coerce").astype("Int64")
    if "trimestre" in df_all.columns:
        df_all["trimestre"] = df_all["trimestre"].map(to_clean_str)

    # Conversion en Table Arrow
    table = pa.Table.from_pandas(df_all, preserve_index=False)

    write_parquet_partitioned(
        table=table,
        bucket=SILVER_BUCKET,
        prefix="dpe",
        partition_cols=["tv016_departement_code", "annee", "trimestre"],
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("=== BRONZE -> SILVER (start) ===")
    logger.info("=== DVF -> SILVER ===")
    transform_dvf()

    logger.info("=== DPE -> SILVER ===")
    transform_dpe()

    logger.info("=== BRONZE -> SILVER (done) ===")


if __name__ == "__main__":
    main()
