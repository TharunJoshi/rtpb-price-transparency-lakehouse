import pandas as pd
import os
import sys

# Resolve repo root (…/rtpb-price-transparency-lakehouse)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)

# Try two possible locations for the CSVs
CANDIDATE_DIRS = [
    os.path.join(REPO_ROOT, "data", "raw"),
    os.path.join(REPO_ROOT, "data", "sample"),
]

DATA_DIR = None
for d in CANDIDATE_DIRS:
    if os.path.isdir(d):
        DATA_DIR = d
        break

if DATA_DIR is None:
    print("❌ Could not find data directory. Checked:")
    for d in CANDIDATE_DIRS:
        print(f"  - {d}")
    sys.exit(1)

print(f"📂 Using DATA_DIR = {DATA_DIR}")

OUTPUT_DIR = os.path.join(REPO_ROOT, "data", "bronze")
os.makedirs(OUTPUT_DIR, exist_ok=True)

tables = [
    "dim_member.csv",
    "dim_plan.csv",
    "dim_drug.csv",
    "dim_pharmacy.csv",
    "dim_prescriber.csv",
    "fact_rtpb_request.csv",
    "fact_price_quote.csv",
    "fact_covered_alternative.csv",
    "fact_claim.csv",
    "formulary_rule.csv",
    "pa_request.csv",
    "audit_event.csv",
]

for table in tables:
    csv_path = os.path.join(DATA_DIR, table)
    if not os.path.exists(csv_path):
        print(f"⚠️ Skipping missing file: {csv_path}")
        continue

    print(f"[READ] {csv_path}")
    df = pd.read_csv(csv_path)

    bronze_path = os.path.join(
        OUTPUT_DIR,
        table.replace(".csv", ".parquet")
    )
    df.to_parquet(bronze_path, index=False)
    print(f"[BRONZE] {table} -> {bronze_path}")

print("✅ Bronze load (pandas) completed.")
