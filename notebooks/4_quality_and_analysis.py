# Databricks notebook source
# MVP Brasileirão - 04 - Qualidade + Respostas do Problema (SQL/PySpark)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "brasileirao_silver.matches"
FACT = "brasileirao_gold.fact_match"
DIM_TEAM = "brasileirao_gold.dim_team"

df = spark.table(SILVER)

# COMMAND ----------
# ===== A) Qualidade de Dados =====
total = df.count()

quality = []
for c in df.columns:
    nulls = df.where(F.col(c).isNull()).count()
    quality.append((c, nulls, total, round(nulls/total*100, 2)))

quality_df = spark.createDataFrame(quality, ["column", "nulls", "total_rows", "null_pct"])
display(quality_df.orderBy(F.desc("null_pct")))

# Regras básicas
checks = {
    "placar_negativo": df.where((F.col("mandante_Placar") < 0) | (F.col("visitante_Placar") < 0)).count(),
    "mandante_igual_visitante": df.where(F.col("mandante") == F.col("visitante")).count(),
    "data_nula": df.where(F.col("match_date").isNull()).count(),
}
display(spark.createDataFrame(list(checks.items()), ["check", "bad_rows"]))

# COMMAND ----------
# ===== B) Responder perguntas do objetivo =====

fact = spark.table(FACT)

# 1) Vantagem de mando de campo (pontos e taxa de vitória)
home_adv = (
    fact.agg(
        F.avg("home_points").alias("avg_home_points"),
        F.avg("away_points").alias("avg_away_points"),
        F.avg(F.when(F.col("result_home") == "W", 1).otherwise(0)).alias("home_win_rate"),
        F.avg(F.when(F.col("result_home") == "D", 1).otherwise(0)).alias("draw_rate"),
        F.avg(F.when(F.col("result_home") == "L", 1).otherwise(0)).alias("away_win_rate"),
    )
)
display(home_adv)

# 2) Tendência de gols ao longo do tempo (por temporada)
goals_by_season = (
    fact.groupBy("season_sk")
        .agg(
            F.count("*").alias("matches"),
            F.avg((F.col("home_goals")+F.col("away_goals")).cast("double")).alias("avg_goals_per_match")
        )
        .orderBy("season_sk")
)
display(goals_by_season)

# 3) Desempenho por clube e temporada (pontos)
# Explode partidas em duas linhas: uma para mandante e outra para visitante
home_rows = fact.select(
    F.col("season_sk").alias("season_year"),
    F.col("home_team_sk").alias("team_sk"),
    F.col("home_points").alias("points"),
    F.col("home_goals").alias("goals_for"),
    F.col("away_goals").alias("goals_against")
)

away_rows = fact.select(
    F.col("season_sk").alias("season_year"),
    F.col("away_team_sk").alias("team_sk"),
    F.col("away_points").alias("points"),
    F.col("away_goals").alias("goals_for"),
    F.col("home_goals").alias("goals_against")
)

team_season = (
    home_rows.union(away_rows)
    .groupBy("season_year", "team_sk")
    .agg(
        F.sum("points").alias("points"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against"),
    )
    .withColumn("goal_diff", F.col("goals_for") - F.col("goals_against"))
)

# ranking por temporada
w = Window.partitionBy("season_year").orderBy(F.desc("points"), F.desc("goal_diff"), F.desc("goals_for"))
ranked = team_season.withColumn("position", F.row_number().over(w))

dim_team = spark.table(DIM_TEAM)
ranked_named = ranked.join(dim_team, on="team_sk", how="left").orderBy("season_year", "position")

display(ranked_named.filter(F.col("season_year") >= 2015).limit(200))
