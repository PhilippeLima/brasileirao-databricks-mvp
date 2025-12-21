# Databricks notebook source
# MVP Brasileirão - 02 - Bronze -> Silver (limpeza + padronização)

# COMMAND ----------
import re
from pyspark.sql import functions as F

# COMMAND ----------
CATALOG = "workspace"
SCHEMA  = "default"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.matches_bronze"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.matches_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
df = spark.table(BRONZE_TABLE)

# COMMAND ----------
# ===== Função: normalizar nome de colunas para snake_case =====
def snake(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"__+", "_", s)
    return s.lower().strip("_")

new_cols = [snake(c) for c in df.columns]
df = df.toDF(*new_cols)

# COMMAND ----------
# ===== Tipos e regras comuns (tentando detectar colunas existentes) =====
cols = set(df.columns)

# datas: tenta achar coluna "data" (mais comum)
if "data" in cols:
    # tenta converter data (funciona com muitos formatos, e se falhar vira null)
    df = df.withColumn("data_dt", F.to_date(F.col("data")))
    # se ficar muito null, tenta dd/MM/yyyy também
    null_ratio = df.filter(F.col("data_dt").isNull()).count() / max(df.count(), 1)
    if null_ratio > 0.5:
        df = df.withColumn("data_dt", F.to_date(F.col("data"), "dd/MM/yyyy"))

# placares
for c in ["mandante_placar", "visitante_placar", "gols_mandante", "gols_visitante"]:
    if c in cols:
        df = df.withColumn(c, F.col(c).cast("int"))

# nomes de times padrão (remove espaços duplos)
for c in ["mandante", "visitante", "vencedor"]:
    if c in cols:
        df = df.withColumn(c, F.trim(F.regexp_replace(F.col(c).cast("string"), r"\s+", " ")))

# cria "temporada" se não existir (usa ano da data_dt)
cols = set(df.columns)
if "temporada" not in cols:
    if "data_dt" in cols:
        df = df.withColumn("temporada", F.year(F.col("data_dt")))
    else:
        df = df.withColumn("temporada", F.lit(None).cast("int"))

# remove duplicados (se existir id, usa; senão usa combinação)
cols = set(df.columns)
if "id" in cols:
    df = df.dropDuplicates(["id"])
else:
    keys = [c for c in ["temporada", "rodada", "mandante", "visitante", "data_dt"] if c in cols]
    if keys:
        df = df.dropDuplicates(keys)

df = df.withColumn("_silver_ts", F.current_timestamp())

# COMMAND ----------
display(df.limit(10))
print("Linhas silver:", df.count(), " | Colunas:", len(df.columns))

# COMMAND ----------
# ===== Gravar Silver =====
(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(SILVER_TABLE)
)

print("OK -> Silver gravado:", SILVER_TABLE)
