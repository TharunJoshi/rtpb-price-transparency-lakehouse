"""
Bronze -> Silver transforms for RTPB:
- fact_price_quote_silver
- fact_covered_alternative_silver
- rtpb_request_silver (light normalization)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, lit
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(REPO_ROOT, "data", "bronze")
SILVER_PATH = os.path.join(REPO_ROOT, "data", "silver")

PHI_COLUMN_CANDIDATES = {
    "name", "first_name", "last_name", "full_name",
    "address", "street", "city", "county", "zip",
    "birth_date", "dob", "admission_date", "discharge_date", "death_date",
    "prescription_date", "phone", "phone_number", "fax", "email",
    "ssn", "social_security_number", "medical_record_number", "mrn",
    "health_plan_beneficiary_number", "subscriber_id",
    "account_number", "billing_account_number",
    "license_number", "certificate_number", "driver_license",
    "vehicle_identifier", "vin", "license_plate",
    "device_identifier", "device_serial",
    "url", "web_url", "portal_url",
    "ip_address",
    "biometric", "fingerprint", "voice_print", "retinal_scan",
    "photo", "full_face_photo", "patient_identifier"
}

def drop_phi_columns(df):
    cols_to_drop = [c for c in df.columns if c.lower() in PHI_COLUMN_CANDIDATES]
    if cols_to_drop:
        print(f"Dropping PHI-like columns: {cols_to_drop}")
        return df.drop(*cols_to_drop)
    return df

def main():
    spark = (
        SparkSession.builder
        .appName("RTPB_Bronze_To_Silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read_bronze(name: str):
        return spark.read.parquet(os.path.join(BRONZE_PATH, name))

    # dims for enrichment
    dim_plan = read_bronze("dim_plan")
    dim_drug = read_bronze("dim_drug")

    # --- Price Quote Silver ---
    price = drop_phi_columns(read_bronze("fact_price_quote"))

    price_silver = (
        price
        .withColumn("response_timestamp_utc", to_timestamp("response_timestamp_utc"))
        .withColumn("is_commercial", when(col("patient_plan_type") == "Commercial", lit("Y")).otherwise(lit("N")))
        .withColumn("is_dod", when(col("patient_plan_type") == "DoD", lit("Y")).otherwise(lit("N")))
        .withColumn("high_cost_flag", when(col("price_quote") >= 100.0, lit("Y")).otherwise(lit("N")))
        .join(dim_plan.select("plan_id", "metal_tier"), on="plan_id", how="left")
        .join(dim_drug.select("ndc", "drug_class", "therapeutic_area", "is_generic"), 
              price.ndc == dim_drug.ndc, how="left")
        .select(
            "transaction_id",
            "patient_plan_type",
            "is_commercial",
            "is_dod",
            "plan_id",
            "metal_tier",
            col("drug_ndc").alias("ndc"),
            "drug_class",
            "therapeutic_area",
            "is_generic",
            "pharmacy_type",
            "requested_dosage",
            "requested_quantity",
            "price_quote",
            "prior_auth_required",
            "step_therapy_flag",
            "deductible_remaining",
            "oop_remaining",
            "high_cost_flag",
            "response_timestamp_utc"
        )
    )

    price_out = os.path.join(SILVER_PATH, "fact_price_quote_silver")
    price_silver.write.mode("overwrite").parquet(price_out)
    print(f"[SILVER] fact_price_quote_silver -> {price_out}")

    # --- Covered Alternative Silver ---
    alts = drop_phi_columns(read_bronze("fact_covered_alternative"))
    alts_silver = (
        alts
        .select(
            "alt_row_id",
            "transaction_id",
            "alt_rank",
            "alt_drug_ndc",
            "alt_drug_name",
            "alt_drug_type",
            "alt_dosage",
            "alt_quantity",
            "alt_pharmacy_type",
            "alt_price_quote",
            "alt_prior_auth_required",
            "formulary_tier",
            "is_preferred"
        )
    )

    alts_out = os.path.join(SILVER_PATH, "fact_covered_alternative_silver")
    alts_silver.write.mode("overwrite").parquet(alts_out)
    print(f"[SILVER] fact_covered_alternative_silver -> {alts_out}")

    # --- RTPB Request Silver (light normalization) ---
    req = drop_phi_columns(read_bronze("fact_rtpb_request"))
    req_silver = (
        req
        .withColumn("ts_request_utc", to_timestamp("ts_request_utc"))
        .select(
            "transaction_id",
            "ts_request_utc",
            "member_sk",
            "plan_sk",
            "prescriber_sk",
            "pharmacy_sk",
            "ndc",
            "days_supply",
            "quantity",
            "channel",
            "request_origin",
            "is_test",
            "correlation_id",
            "request_status"
        )
    )
    req_out = os.path.join(SILVER_PATH, "fact_rtpb_request_silver")
    req_silver.write.mode("overwrite").parquet(req_out)
    print(f"[SILVER] fact_rtpb_request_silver -> {req_out}")

    spark.stop()

if __name__ == "__main__":
    main()
