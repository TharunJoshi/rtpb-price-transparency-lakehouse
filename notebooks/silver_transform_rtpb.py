# Databricks notebook source
# 02_silver_transform_rtpb

from pyspark.sql.functions import col, when, to_timestamp, lit

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("bronze_schema", "rtpb_bronze", "Bronze Schema")
dbutils.widgets.text("silver_schema", "rtpb_silver", "Silver Schema")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

PHI_COLUMN_CANDIDATES = {
    "name","first_name","last_name","full_name",
    "address","street","city","county","zip",
    "birth_date","dob","admission_date","discharge_date","death_date","prescription_date",
    "phone","phone_number","fax","email",
    "ssn","social_security_number",
    "medical_record_number","mrn",
    "health_plan_beneficiary_number","subscriber_id",
    "account_number","billing_account_number",
    "license_number","certificate_number","driver_license",
    "vehicle_identifier","vin","license_plate",
    "device_identifier","device_serial",
    "url","web_url","portal_url",
    "ip_address",
    "biometric","fingerprint","voice_print","retinal_scan",
    "photo","full_face_photo","patient_identifier"
}

def drop_phi_columns(df):
    cols_to_drop = [c for c in df.columns if c.lower() in PHI_COLUMN_CANDIDATES]
    if cols_to_drop:
        print(f"Dropping PHI-like columns: {cols_to_drop}")
        return df.drop(*cols_to_drop)
    return df

def t(table):  # helper
    return f"{catalog}.{bronze_schema}.{table}"

dim_plan = spark.table(t("dim_plan"))
dim_drug = spark.table(t("dim_drug"))

# --- Silver Price Quote ---
bronze_price = drop_phi_columns(spark.table(t("fact_price_quote")))

price_silver = (
    bronze_price
    .withColumn("response_timestamp_utc", to_timestamp("response_timestamp_utc"))
    .withColumn("is_commercial", when(col("patient_plan_type") == "Commercial", lit("Y")).otherwise(lit("N")))
    .withColumn("is_dod", when(col("patient_plan_type") == "DoD", lit("Y")).otherwise(lit("N")))
    .withColumn("high_cost_flag", when(col("price_quote") >= 100.0, lit("Y")).otherwise(lit("N")))
    .join(dim_plan.select("plan_id", "metal_tier"), on="plan_id", how="left")
    .join(dim_drug.select("ndc", "drug_class", "therapeutic_area", "is_generic"),
          col("drug_ndc") == dim_drug.ndc, "left")
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

(
    price_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{silver_schema}.fact_price_quote_silver")
)

# --- Silver Covered Alternatives ---
bronze_alts = drop_phi_columns(spark.table(t("fact_covered_alternative")))

alts_silver = (
    bronze_alts
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

(
    alts_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{silver_schema}.fact_covered_alternative_silver")
)

display(spark.table(f"{catalog}.{silver_schema}.fact_price_quote_silver").limit(5))
display(spark.table(f"{catalog}.{silver_schema}.fact_covered_alternative_silver").limit(5))
