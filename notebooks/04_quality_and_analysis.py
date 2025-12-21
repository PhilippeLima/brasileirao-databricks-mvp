# Databricks notebook source
# MVP Brasileirão - 04 - Consultas / respostas do objetivo

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
CATALOG = "workspace"
SCHEMA  = "default"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.matches_silver"
GOLD_TABLE   = f"{CATALOG}.{SCHEMA}.team_season_gold"

df_matches = spark.table(SILVER_TABLE)
df_gold    = spark.table(GOLD_TABLE)

# COMMAND ----------
# 1) Top 10 times por pontos na última temporada disponível
last_season = df_gold.agg(F.max("temporada").alias("t")).collect()[0]["t"]
print("Última temporada:", last_season)

top10 = (df_gold
         .filter(F.col("temporada") == last_season)
         .orderBy(F.desc("pontos"), F.desc("saldo_gols"), F.desc("gols_pro"))
         .limit(10))

display(top10)

# COMMAND ----------
# 2) Média de gols por jogo por temporada
cols = set(df_matches.columns)
home_score_col = "mandante_placar" if "mandante_placar" in cols else ("gols_mandante" if "gols_mandante" in cols else None)
away_score_col = "visitante_placar" if "visitante_placar" in cols else ("gols_visitante" if "gols_visitante" in cols else None)

if home_score_col and away_score_col:
    gols = (df_matches
            .withColumn("gols_total", F.col(home_score_col).cast("int") + F.col(away_score_col).cast("int"))
            .groupBy("temporada")
            .agg(
                F.count("*").alias("jogos"),
                F.avg("gols_total").alias("media_gols_por_jogo")
            )
            .orderBy("temporada"))
    display(gols)
else:
    print("Não encontrei colunas de placar para calcular média de gols.")

# COMMAND ----------
# 3) Vantagem de mandante (percentual vitórias mandante por temporada)
if home_score_col and away_score_col:
    vdm = (df_matches
           .withColumn("resultado",
                       F.when(F.col(home_score_col).cast("int") > F.col(away_score_col).cast("int"), "H")
                        .when(F.col(home_score_col).cast("int") < F.col(away_score_col).cast("int"), "A")
                        .otherwise("D"))
           .groupBy("temporada")
           .agg(
               F.count("*").alias("jogos"),
               F.avg(F.when(F.col("resultado") == "H", 1).otherwise(0)).alias("pct_vitoria_mandante"),
               F.avg(F.when(F.col("resultado") == "D", 1).otherwise(0)).alias("pct_empate"),
               F.avg(F.when(F.col("resultado") == "A", 1).otherwise(0)).alias("pct_vitoria_visitante"),
           )
           .orderBy("temporada"))
    display(vdm)
