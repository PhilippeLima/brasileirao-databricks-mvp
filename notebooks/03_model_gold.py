# Databricks notebook source
# MVP Brasileirão - 03 - Gold (modelo estrela)

from pyspark.sql import functions as F

# COMMAND ----------
SILVER_TABLE = "brasileirao_silver.matches"

GOLD_DB = "brasileirao_gold"
DIM_TEAM = f"{GOLD_DB}.dim_team"
DIM_DATE = f"{GOLD_DB}.dim_date"
DIM_SEASON = f"{GOLD_DB}.dim_season"
FACT_MATCH = f"{GOLD_DB}.fact_match"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

df = spark.table(SILVER_TABLE)

# COMMAND ----------
# ===== Dimensão Time =====
teams = (
    df.select(F.col("mandante").alias("team_name"))
    .union(df.select(F.col("visitante").alias("team_name")))
    .where(F.col("team_name").isNotNull() & (F.col("team_name") != ""))
    .distinct()
)

dim_team = teams.withColumn(
    "team_sk",
    F.sha2(F.lower(F.col("team_name")), 256)
).select("team_sk", "team_name")

dim_team.write.format("delta").mode("overwrite").saveAsTable(DIM_TEAM)
print("Criado:", DIM_TEAM, "linhas:", dim_team.count())

# COMMAND ----------
# ===== Dimensão Data =====
dim_date = (
    df.select("match_date")
    .where(F.col("match_date").isNotNull())
    .distinct()
    .withColumn("date_sk", F.date_format(F.col("match_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("match_date").cast("int"))
    .withColumn("month", F.month("match_date").cast("int"))
    .withColumn("day", F.dayofmonth("match_date").cast("int"))
    .withColumn("dow", F.date_format("match_date", "E"))
    .select("date_sk", "match_date", "year", "month", "day", "dow")
)

dim_date.write.format("delta").mode("overwrite").saveAsTable(DIM_DATE)
print("Criado:", DIM_DATE, "linhas:", dim_date.count())

# COMMAND ----------
# ===== Dimensão Temporada =====
dim_season = (
    df.select("season_year")
    .where(F.col("season_year").isNotNull())
    .distinct()
    .withColumnRenamed("season_year", "season_sk")
    .withColumn("season_year", F.col("season_sk"))
    .select("season_sk", "season_year")
)

dim_season.write.format("delta").mode("overwrite").saveAsTable(DIM_SEASON)
print("Criado:", DIM_SEASON, "linhas:", dim_season.count())

# COMMAND ----------
# ===== Fato Partida =====
fact = (
    df
    .withColumn("home_team_sk", F.sha2(F.lower(F.col("mandante")), 256))
    .withColumn("away_team_sk", F.sha2(F.lower(F.col("visitante")), 256))
    .withColumn("date_sk", F.date_format(F.col("match_date"), "yyyyMMdd").cast("int"))
    .withColumn("season_sk", F.col("season_year"))
    .select(
        F.col("ID").cast("bigint").alias("match_id"),
        "season_sk", "date_sk",
        "home_team_sk", "away_team_sk",
        F.col("rodata").cast("int").alias("round"),
        F.col("arena").alias("stadium"),
        F.col("mandante_Placar").cast("int").alias("home_goals"),
        F.col("visitante_Placar").cast("int").alias("away_goals"),
        F.col("goal_diff").cast("int"),
        F.col("result_home"),
        F.col("home_points").cast("int"),
        F.col("away_points").cast("int"),
        F.col("vencedor").alias("winner_raw"),
    )
)

fact.write.format("delta").mode("overwrite").saveAsTable(FACT_MATCH)
print("Criado:", FACT_MATCH, "linhas:", fact.count())

# COMMAND ----------
# Visualização rápida
display(spark.sql(f"SHOW TABLES IN {GOLD_DB}"))
display(spark.sql(f"SELECT * FROM {FACT_MATCH} LIMIT 10"))
