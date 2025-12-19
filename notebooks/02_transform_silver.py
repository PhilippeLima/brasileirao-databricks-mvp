# Databricks notebook source
# MVP Brasileirão - 02 - Silver (tratamento + enriquecimento)

from pyspark.sql import functions as F

# COMMAND ----------
BRONZE_TABLE = "brasileirao_bronze.matches_raw"
SILVER_DB = "brasileirao_silver"
SILVER_TABLE = f"{SILVER_DB}.matches"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

# COMMAND ----------
df = spark.table(BRONZE_TABLE)

# Normalizar nomes de colunas comuns (caso o dataset venha com variações)
# Aqui assumimos as colunas principais do CSV do Brasileirão.
# Se alguma coluna não existir no seu CSV, o erro vai mostrar qual é — aí ajustamos rapidinho.

# COMMAND ----------
# ===== Conversões e padronizações =====
# - trim em strings
# - placares e rodada para int
# - data para DATE (dd/MM/yyyy)
# - colunas derivadas de resultado e pontos

df_silver = (
    df
    .withColumn("mandante", F.trim(F.col("mandante")))
    .withColumn("visitante", F.trim(F.col("visitante")))
    .withColumn("vencedor", F.trim(F.col("vencedor")))
    .withColumn("arena", F.trim(F.col("arena")))
    .withColumn("rodata", F.col("rodata").cast("int"))
    .withColumn("mandante_Placar", F.col("mandante_Placar").cast("int"))
    .withColumn("visitante_Placar", F.col("visitante_Placar").cast("int"))
    .withColumn("match_date", F.to_date(F.col("data"), "dd/MM/yyyy"))
    .withColumn("season_year", F.year(F.col("match_date")).cast("int"))
    .withColumn("goal_diff", (F.col("mandante_Placar") - F.col("visitante_Placar")).cast("int"))
)

# Resultado do mandante: W/D/L
df_silver = df_silver.withColumn(
    "result_home",
    F.when(F.col("mandante_Placar") > F.col("visitante_Placar"), F.lit("W"))
     .when(F.col("mandante_Placar") < F.col("visitante_Placar"), F.lit("L"))
     .otherwise(F.lit("D"))
)

# Pontos
df_silver = df_silver.withColumn(
    "home_points",
    F.when(F.col("result_home") == "W", F.lit(3))
     .when(F.col("result_home") == "D", F.lit(1))
     .otherwise(F.lit(0))
).withColumn(
    "away_points",
    F.when(F.col("result_home") == "L", F.lit(3))
     .when(F.col("result_home") == "D", F.lit(1))
     .otherwise(F.lit(0))
)

# COMMAND ----------
# ===== Gravar Silver =====
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

print("Tabela Silver criada:", SILVER_TABLE)
display(spark.sql(f"SELECT COUNT(*) AS rows FROM {SILVER_TABLE}"))
display(spark.sql(f"SELECT * FROM {SILVER_TABLE} LIMIT 10"))
