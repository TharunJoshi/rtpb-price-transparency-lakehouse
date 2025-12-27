# Databricks notebook source
# 03_gold_rtpb_kpis

from pyspark.sql.functions import col, avg, sum as _sum, countDistinct, when

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("silver_schema", "rtpb_silver", "Silver Schema")
dbutils.widgets.text("gold_schema", "rtpb_gold", "Gold Schema")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

price = spark.table(f"{catalog}.{silver_schema}.fact_price_quote_silver")
alts  = spark.table(f"{catalog}.{silver_schema}.fact_covered_alternative_silver")

# 1) Average OOP by plan type
avg_oop = (
    price
    .groupBy("patient_plan_type")
    .agg(
        avg("price_quote").alias("avg_price_quote"),
        _sum(when(col("high_cost_flag") == "Y", 1).otherwise(0)).alias("high_cost_txn_cnt"),
        countDistinct("transaction_id").alias("txn_cnt")
    )
)

avg_oop.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.avg_oop_by_plan_type"
)

# 2) Alternative savings
joined = (
    price.alias("p")
    .join(alts.alias("a"), "transaction_id", "inner")
    .select(
        "transaction_id",
        col("p.patient_plan_type"),
        col("p.plan_id"),
        col("p.ndc").alias("requested_ndc"),
        col("p.price_quote").alias("requested_price"),
        "alt_drug_ndc",
        "alt_drug_name",
        "alt_drug_type",
        "alt_price_quote",
    )
    .withColumn("potential_savings", col("requested_price") - col("alt_price_quote"))
)

joined.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.alt_savings_detail"
)

savings_summary = (
    joined
    .groupBy("patient_plan_type", "alt_drug_type")
    .agg(
        avg("requested_price").alias("avg_requested_price"),
        avg("alt_price_quote").alias("avg_alt_price"),
        avg("potential_savings").alias("avg_potential_savings"),
        _sum(when(col("potential_savings") > 0, 1).otherwise(0)).alias("positive_savings_cases"),
        countDistinct("transaction_id").alias("txn_cnt")
    )
)

savings_summary.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.alt_savings_summary"
)

# 3) Prior Auth rates
pa_rates = (
    price
    .groupBy("patient_plan_type")
    .agg(
        _sum(when(col("prior_auth_required") == "Y", 1).otherwise(0)).alias("pa_required_cnt"),
        countDistinct("transaction_id").alias("txn_cnt")
    )
)

pa_rates.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{gold_schema}.prior_auth_rates_by_plan_type"
)

display(spark.table(f"{catalog}.{gold_schema}.avg_oop_by_plan_type"))
display(spark.table(f"{catalog}.{gold_schema}.alt_savings_summary"))
display(spark.table(f"{catalog}.{gold_schema}.prior_auth_rates_by_plan_type"))
