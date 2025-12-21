# Databricks notebook source
# MVP Brasileirão - 01 - Ingestão (CSV) -> Bronze (Delta/UC)

# COMMAND ----------
import os
import requests

from pyspark.sql import functions as F

# COMMAND ----------
# ===== Config (Unity Catalog + Volume) =====
CATALOG = "workspace"
SCHEMA  = "default"
VOLUME  = "mvp"  # você cria no Catalog > workspace > default > Create > Volume

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_DIR     = f"{VOLUME_PATH}/raw"
RAW_PATH    = f"{RAW_DIR}/campeonato-brasileiro-full.csv"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.matches_bronze"

DATA_URL = "https://raw.githubusercontent.com/adaodque/Brasileirao_Dataset/master/campeonato-brasileiro-full.csv"

# garante schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# garante pasta no volume
os.makedirs(RAW_DIR, exist_ok=True)

print("RAW_PATH:", RAW_PATH)
print("BRONZE_TABLE:", BRONZE_TABLE)

# COMMAND ----------
# ===== Baixar o CSV (se não existir) =====
if not os.path.exists(RAW_PATH) or os.path.getsize(RAW_PATH) == 0:
    r = requests.get(DATA_URL, timeout=120)
    r.raise_for_status()
    with open(RAW_PATH, "wb") as f:
        f.write(r.content)

print("Arquivo pronto:", RAW_PATH, "bytes:", os.path.getsize(RAW_PATH))

# COMMAND ----------
# ===== Ler CSV -> DataFrame =====
df_raw = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_PATH)
)

df_raw = df_raw.withColumn("_ingest_ts", F.current_timestamp())

display(df_raw.limit(10))
print("Linhas:", df_raw.count(), " | Colunas:", len(df_raw.columns))

# COMMAND ----------
# ===== Escrever Bronze Delta (UC) =====
(
    df_raw.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE)
)

print("OK -> Bronze gravado:", BRONZE_TABLE)
