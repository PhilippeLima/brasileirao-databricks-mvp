# Databricks notebook source
# MVP Brasileirão - 01 - Ingestão e Bronze (Delta)

from pyspark.sql import functions as F

# COMMAND ----------
# ===== Parâmetros =====
# URL do dataset (se der erro ao baixar, faça upload manual e só ajuste RAW_DBFS_PATH)
DATA_URL = "https://raw.githubusercontent.com/adaoduque/Brasileirao_Dataset/master/campeonato-brasileiro-full.csv"

RAW_DIR_DBFS = "dbfs:/FileStore/brasileirao/raw"
RAW_DBFS_PATH = f"{RAW_DIR_DBFS}/campeonato-brasileiro-full.csv"

BRONZE_DB = "brasileirao_bronze"
BRONZE_TABLE = f"{BRONZE_DB}.matches_raw"

print("RAW_DBFS_PATH =", RAW_DBFS_PATH)

# COMMAND ----------
# ===== 1) Garantir pasta no DBFS =====
dbutils.fs.mkdirs(RAW_DIR_DBFS)
display(dbutils.fs.ls("dbfs:/FileStore/brasileirao/"))

# COMMAND ----------
# ===== 2) Baixar CSV (opcional) =====
# Se o download falhar por rede/restrição, faça upload manual no Databricks:
# Data -> Add data -> Upload file -> e coloque no FileStore/brasileirao/raw/
# Depois reexecute a célula 3 (leitura).

import os, urllib.request

local_tmp = "/tmp/campeonato-brasileiro-full.csv"

def file_exists_dbfs(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

if not file_exists_dbfs(RAW_DBFS_PATH):
    print("Arquivo não encontrado no DBFS. Tentando baixar do GitHub...")
    try:
        urllib.request.urlretrieve(DATA_URL, local_tmp)
        dbutils.fs.cp(f"file:{local_tmp}", RAW_DBFS_PATH)
        print("Download OK e copiado para DBFS:", RAW_DBFS_PATH)
    except Exception as e:
        print("Falhou o download automático.")
        print("Faça upload manual do CSV para:", RAW_DBFS_PATH)
        raise e
else:
    print("Arquivo já existe no DBFS. OK.")

# COMMAND ----------
# ===== 3) Ler CSV (raw) =====
df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW_DBFS_PATH)
)

print("Linhas:", df_raw.count())
display(df_raw.limit(10))

# COMMAND ----------
# ===== 4) Criar database Bronze e salvar Delta =====
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

(
    df_raw
    .withColumn("ingestion_ts", F.current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

print("Tabela Bronze criada:", BRONZE_TABLE)
display(spark.sql(f"SELECT COUNT(*) AS rows FROM {BRONZE_TABLE}"))
