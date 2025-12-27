"""
Silver -> Gold analytics for RTPB:
- avg_oop_by_plan_type
- alt_savings_summary
- prior_auth_rates_by_plan_type
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, countDistinct, when
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_PATH = os.path.join(REPO_ROOT, "data", "silver")
GOLD_PATH = os.path.join(REPO_ROOT, "data", "gold")

def main():
    spark = (
        SparkSession.builder
        .appName("RTPB_Silver_To_Gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read_silver(name: str):
        return spark.read.parquet(os.path.join(SILVER_PATH, name))

    price_silver = read_silver("fact_price_quote_silver")
    alts_silver  = read_silver("fact_covered_alternative_silver")

    # 1) Avg OOP & high-cost counts by plan type
    avg_oop_by_plan_type = (
        price_silver
        .groupBy("patient_plan_type")
        .agg(
            avg("price_quote").alias("avg_price_quote"),
            _sum(when(col("high_cost_flag") == "Y", 1).otherwise(0)).alias("high_cost_txn_cnt"),
            countDistinct("transaction_id").alias("txn_cnt")
        )
    )
    out1 = os.path.join(GOLD_PATH, "avg_oop_by_plan_type")
    avg_oop_by_plan_type.write.mode("overwrite").parquet(out1)
    print(f"[GOLD] avg_oop_by_plan_type -> {out1}")

    # 2) Alternative savings detail + summary
    joined = (
        price_silver.alias("p")
        .join(alts_silver.alias("a"), "transaction_id", "inner")
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

    out_detail = os.path.join(GOLD_PATH, "alt_savings_detail")
    joined.write.mode("overwrite").parquet(out_detail)
    print(f"[GOLD] alt_savings_detail -> {out_detail}")

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
    out_summary = os.path.join(GOLD_PATH, "alt_savings_summary")
    savings_summary.write.mode("overwrite").parquet(out_summary)
    print(f"[GOLD] alt_savings_summary -> {out_summary}")

    # 3) Prior-Auth rates
    pa_rates = (
        price_silver
        .groupBy("patient_plan_type")
        .agg(
            _sum(when(col("prior_auth_required") == "Y", 1).otherwise(0)).alias("pa_required_cnt"),
            countDistinct("transaction_id").alias("txn_cnt")
        )
    )
    out_pa = os.path.join(GOLD_PATH, "prior_auth_rates_by_plan_type")
    pa_rates.write.mode("overwrite").parquet(out_pa)
    print(f"[GOLD] prior_auth_rates_by_plan_type -> {out_pa}")

    spark.stop()

if __name__ == "__main__":
    main()
