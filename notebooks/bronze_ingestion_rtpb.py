# Databricks notebook source
# 01_bronze_ingestion_rtpb – load 12 CSVs to Delta Bronze

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("bronze_schema", "rtpb_bronze", "Bronze Schema")
dbutils.widgets.text("landing_base_path", "dbfs:/FileStore/rtpb", "Landing Base Path")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
landing_base = dbutils.widgets.get("landing_base_path")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

tables = [
    "dim_member",
    "dim_plan",
    "dim_drug",
    "dim_pharmacy",
    "dim_prescriber",
    "fact_rtpb_request",
    "fact_price_quote",
    "fact_covered_alternative",
    "fact_claim",
    "formulary_rule",
    "pa_request",
    "audit_event",
]

for t in tables:
    path = f"{landing_base}/sample/{t}.csv"
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{bronze_schema}.{t}")
    )
    print(f"Loaded {t} from {path} into {catalog}.{bronze_schema}.{t}")

display(spark.table(f"{catalog}.{bronze_schema}.fact_price_quote").limit(5))
display(spark.table(f"{catalog}.{bronze_schema}.fact_covered_alternative").limit(5))
