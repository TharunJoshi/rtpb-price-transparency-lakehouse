"""
Ingestion script: 12 CSVs -> Bronze Parquet (Delta-style layout).
This is for local / portfolio use (no PHI fields).
"""

from pyspark.sql import SparkSession
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SAMPLE_PATH = os.path.join(REPO_ROOT, "data", "sample")
BRONZE_PATH = os.path.join(REPO_ROOT, "data", "bronze")

TABLES = [
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

def main():
    spark = (
        SparkSession.builder
        .appName("RTPB_Bronze_Ingestion")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        csv_path = os.path.join(SAMPLE_PATH, f"{table}.csv")
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(csv_path)
        )

        # Write as Parquet partition per table
        out_path = os.path.join(BRONZE_PATH, table)
        (
            df.write
            .mode("overwrite")
            .parquet(out_path)
        )
        print(f"[BRONZE] Loaded {table} -> {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
