import os
import pandas as pd

# ====== Paths ======
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)

SILVER_DIR = os.path.join(REPO_ROOT, "data", "silver")
GOLD_DIR = os.path.join(REPO_ROOT, "data", "gold")

os.makedirs(GOLD_DIR, exist_ok=True)

print(f"📂 SILVER_DIR = {SILVER_DIR}")
print(f"📂 GOLD_DIR   = {GOLD_DIR}")

# ====== Helpers ======

def safe_read_parquet(path: str, label: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"⚠️ {label}: parquet not found at {path}. Returning empty DataFrame.")
        return pd.DataFrame()
    print(f"[LOAD] {label}: {path}")
    return pd.read_parquet(path)


# ====== Load Silver tables ======

price = safe_read_parquet(
    os.path.join(SILVER_DIR, "price_silver.parquet"),
    "price_silver"
)
alt = safe_read_parquet(
    os.path.join(SILVER_DIR, "alt_silver.parquet"),
    "alt_silver"
)
claims = safe_read_parquet(
    os.path.join(SILVER_DIR, "claims_silver.parquet"),
    "claims_silver"
)

if price.empty:
    print("❌ price_silver is empty or missing. Cannot compute KPIs.")
    raise SystemExit(1)

# We will use price as primary fact for KPIs:
# - patient_plan_type (Commercial vs DoD)
# - plan_id
# - price_quote
# - transaction_id

# ====== KPI 1: Average out-of-pocket cost by plan & plan type ======

print("🔹 KPI 1: avg_oop_by_plan_type...")

group_cols = []
if "plan_id" in price.columns:
    group_cols.append("plan_id")
if "patient_plan_type" in price.columns:
    group_cols.append("patient_plan_type")

if not group_cols:
    print("⚠️ Neither 'plan_id' nor 'patient_plan_type' found in price_silver. "
          "Cannot compute avg_oop_by_plan_type.")
    avg_oop_by_plan = pd.DataFrame()
else:
    avg_oop_by_plan = (
        price.groupby(group_cols)["price_quote"]
        .mean()
        .reset_index()
        .rename(columns={"price_quote": "avg_price_quote"})
    )
    out_path = os.path.join(GOLD_DIR, "avg_oop_by_plan.parquet")
    avg_oop_by_plan.to_parquet(out_path, index=False)
    print(f"[GOLD] avg_oop_by_plan -> {out_path}")

# ====== KPI 2: DoD vs Commercial average OOP ======

print("🔹 KPI 2: dod_vs_commercial_oop...")

if "patient_plan_type" in price.columns:
    dod_vs_comm = (
        price.groupby("patient_plan_type")["price_quote"]
        .mean()
        .reset_index()
        .rename(columns={"price_quote": "avg_price_quote"})
    )
    out_path = os.path.join(GOLD_DIR, "dod_vs_commercial_oop.parquet")
    dod_vs_comm.to_parquet(out_path, index=False)
    print(f"[GOLD] dod_vs_commercial_oop -> {out_path}")
else:
    print("⚠️ Column 'patient_plan_type' not found in price_silver. "
          "Cannot compute DoD vs Commercial KPI.")
    dod_vs_comm = pd.DataFrame()

# ====== KPI 3: Generic switch rate by plan (if alt table is available) ======

print("🔹 KPI 3: generic_switch_rate_by_plan...")

generic_switch = pd.DataFrame()

if (
    not alt.empty
    and "transaction_id" in price.columns
    and "transaction_id" in alt.columns
    and "price_quote" in price.columns
    and "alt_price_quote" in alt.columns
):
    # minimal columns
    left_cols = ["transaction_id", "price_quote"]
    if "plan_id" in price.columns:
        left_cols.append("plan_id")
    if "patient_plan_type" in price.columns:
        left_cols.append("patient_plan_type")

    right_cols = ["transaction_id", "alt_price_quote"]
    if "alt_drug_type" in alt.columns:
        right_cols.append("alt_drug_type")

    joined = (
        price[left_cols]
        .merge(alt[right_cols], on="transaction_id", how="left")
    )

    # define a "cheaper generic" switch
    if "alt_drug_type" in joined.columns:
        joined["is_generic_alt"] = joined["alt_drug_type"].str.lower().eq("generic")
    else:
        joined["is_generic_alt"] = False

    joined["is_cheaper_generic_switch"] = (
        joined["is_generic_alt"]
        & joined["alt_price_quote"].notna()
        & (joined["alt_price_quote"] < joined["price_quote"])
    )

    # group by plan (and plan type if available)
    kpi_group_cols = []
    if "plan_id" in joined.columns:
        kpi_group_cols.append("plan_id")
    if "patient_plan_type" in joined.columns:
        kpi_group_cols.append("patient_plan_type")

    if not kpi_group_cols:
        print("⚠️ No plan-level columns in joined price/alt. "
              "Computing a global generic switch KPI only.")
        grouped = joined.assign(_all="ALL").groupby("_all")
    else:
        grouped = joined.groupby(kpi_group_cols)

    metric = grouped.agg(
        total_txn=("transaction_id", "nunique"),
        generic_switches=("is_cheaper_generic_switch", "sum"),
    ).reset_index()

    metric["generic_switch_rate"] = (
        metric["generic_switches"] / metric["total_txn"]
    )

    generic_switch = metric

    out_path = os.path.join(GOLD_DIR, "generic_switch_rate_by_plan.parquet")
    generic_switch.to_parquet(out_path, index=False)
    print(f"[GOLD] generic_switch_rate_by_plan -> {out_path}")
else:
    print("⚠️ Cannot compute generic_switch_rate_by_plan because one of "
          "[alt, transaction_id, price_quote, alt_price_quote] is missing.")

print("✅ Gold KPIs (pandas) computation completed.")
