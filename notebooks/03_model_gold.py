# Databricks notebook source
# MVP Brasileirão - 03 - Silver -> Gold (métricas por time/temporada)

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
CATALOG = "workspace"
SCHEMA  = "default"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.matches_silver"
GOLD_TABLE   = f"{CATALOG}.{SCHEMA}.team_season_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
df = spark.table(SILVER_TABLE)
cols = set(df.columns)

# valida colunas mínimas
need = {"mandante", "visitante"}
missing = [c for c in need if c not in cols]
if missing:
    raise Exception(f"Faltam colunas essenciais no Silver: {missing}")

# tenta identificar placares
home_score_col = "mandante_placar" if "mandante_placar" in cols else ("gols_mandante" if "gols_mandante" in cols else None)
away_score_col = "visitante_placar" if "visitante_placar" in cols else ("gols_visitante" if "gols_visitante" in cols else None)

if home_score_col is None or away_score_col is None:
    raise Exception("Não encontrei colunas de placar (mandante_placar/visitante_placar ou gols_mandante/gols_visitante).")

# temporada
season_col = "temporada" if "temporada" in cols else None
if season_col is None:
    raise Exception("Não encontrei coluna 'temporada' no Silver.")

# cria resultado por partida (mandante x visitante)
df2 = df.withColumn("home_goals", F.col(home_score_col).cast("int")) \
        .withColumn("away_goals", F.col(away_score_col).cast("int")) \
        .withColumn("resultado",
                    F.when(F.col("home_goals") > F.col("away_goals"), F.lit("H"))
                     .when(F.col("home_goals") < F.col("away_goals"), F.lit("A"))
                     .otherwise(F.lit("D"))
                   )

# COMMAND ----------
# ===== Tabela "long" por time (uma linha para mandante + uma para visitante) =====
home = df2.select(
    F.col(season_col).alias("temporada"),
    F.col("mandante").alias("time"),
    F.lit(1).alias("jogos"),
    F.when(F.col("resultado") == "H", 1).otherwise(0).alias("vitorias"),
    F.when(F.col("resultado") == "D", 1).otherwise(0).alias("empates"),
    F.when(F.col("resultado") == "A", 1).otherwise(0).alias("derrotas"),
    F.col("home_goals").alias("gols_pro"),
    F.col("away_goals").alias("gols_contra")
)

away = df2.select(
    F.col(season_col).alias("temporada"),
    F.col("visitante").alias("time"),
    F.lit(1).alias("jogos"),
    F.when(F.col("resultado") == "A", 1).otherwise(0).alias("vitorias"),
    F.when(F.col("resultado") == "D", 1).otherwise(0).alias("empates"),
    F.when(F.col("resultado") == "H", 1).otherwise(0).alias("derrotas"),
    F.col("away_goals").alias("gols_pro"),
    F.col("home_goals").alias("gols_contra")
)

long_df = home.unionByName(away)

gold = (long_df
        .groupBy("temporada", "time")
        .agg(
            F.sum("jogos").alias("jogos"),
            F.sum("vitorias").alias("vitorias"),
            F.sum("empates").alias("empates"),
            F.sum("derrotas").alias("derrotas"),
            F.sum("gols_pro").alias("gols_pro"),
            F.sum("gols_contra").alias("gols_contra")
        )
        .withColumn("saldo_gols", F.col("gols_pro") - F.col("gols_contra"))
        .withColumn("pontos", F.col("vitorias")*3 + F.col("empates"))
        .withColumn("_gold_ts", F.current_timestamp())
)

# COMMAND ----------
display(gold.orderBy(F.desc("temporada"), F.desc("pontos")).limit(30))

# COMMAND ----------
# ===== Gravar Gold =====
(
    gold.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_TABLE)
)

print("OK -> Gold gravado:", GOLD_TABLE)
