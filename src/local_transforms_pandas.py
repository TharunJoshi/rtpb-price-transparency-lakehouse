import os
import sys
import pandas as pd

# Resolve repo root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)

BRONZE_DIR = os.path.join(REPO_ROOT, "data", "bronze")
SILVER_DIR = os.path.join(REPO_ROOT, "data", "silver")

RAW_DIR_CANDIDATES = [
    os.path.join(REPO_ROOT, "data", "raw"),
    os.path.join(REPO_ROOT, "data", "sample"),
]

os.makedirs(SILVER_DIR, exist_ok=True)


def find_raw_dir() -> str:
    """Pick the first existing raw/sample data directory."""
    for d in RAW_DIR_CANDIDATES:
        if os.path.isdir(d):
            return d
    print("❌ Could not find raw/sample data directory. Checked:")
    for d in RAW_DIR_CANDIDATES:
        print(f"  - {d}")
    sys.exit(1)


RAW_DIR = find_raw_dir()
print(f"📂 Using RAW_DIR = {RAW_DIR}")


def load_required(name: str) -> pd.DataFrame:
    """
    Try to load from bronze parquet first; if missing, fall back to CSV in RAW_DIR.
    If neither exists, exit with error.
    """
    parquet_path = os.path.join(BRONZE_DIR, f"{name}.parquet")
    csv_path = os.path.join(RAW_DIR, f"{name}.csv")

    if os.path.exists(parquet_path):
        print(f"[LOAD] {name} from bronze parquet: {parquet_path}")
        return pd.read_parquet(parquet_path)

    if os.path.exists(csv_path):
        print(f"[LOAD] {name} from raw/sample CSV: {csv_path}")
        return pd.read_csv(csv_path)

    print(f"❌ Neither parquet nor CSV found for required table '{name}'.")
    print(f"  Tried: {parquet_path}")
    print(f"  Tried: {csv_path}")
    sys.exit(1)


def detect_join_key(left: pd.DataFrame, right: pd.DataFrame) -> tuple[str, str]:
    """
    Detect a sensible join key between fact and dim tables.

    Priority:
    1) Member-level: member_id, member_sk, member_key, member_unique_id, member_unique_seq_nbr
    2) Plan-level:   plan_id, plan_sk, plan_key
    """
    member_candidates = [
        "member_id",
        "member_sk",
        "member_key",
        "member_unique_id",
        "member_unique_seq_nbr",
    ]
    plan_candidates = [
        "plan_id",
        "plan_sk",
        "plan_key",
    ]

    # Member-level first
    for col in member_candidates:
        if col in left.columns and col in right.columns:
            return col, "member"

    # Then plan-level
    for col in plan_candidates:
        if col in left.columns and col in right.columns:
            return col, "plan"

    return "", ""


print("🔹 Loading source tables...")
price = load_required("fact_price_quote")
alt = load_required("fact_covered_alternative")
claims = load_required("fact_claim")
members = load_required("dim_member")  # present & modeled

print("🔹 Detecting join key between facts and dim_member...")
join_col, join_level = detect_join_key(price, members)

if not join_col:
    print("❌ Could not find a common join column between fact tables and dim_member.")
    print(f"  price columns:   {list(price.columns)}")
    print(f"  members columns: {list(members.columns)}")
    sys.exit(1)

print(f"✅ Using '{join_col}' as {join_level}-level join key.")



# ---- Build silver tables ----

def build_silver(df: pd.DataFrame, name: str) -> None:
    print(f"🔹 Building silver_{name}...")

    if join_col in df.columns:
        enriched = df.merge(members, on=join_col, how="left")
        print(f"   ✅ Joined {name} with dim_member on '{join_col}'")
    else:
        enriched = df.copy()
        print(
            f"   ⚠️ Column '{join_col}' not found in {name} fact. "
            f"Writing {name} to silver without member/plan enrichment."
        )

    out_path = os.path.join(SILVER_DIR, f"{name}_silver.parquet")
    enriched.to_parquet(out_path, index=False)
    print(f"[SILVER] {name}_silver -> {out_path}")


build_silver(price, "price")
build_silver(alt, "alt")
build_silver(claims, "claims")

print("✅ Silver transformations (pandas) completed.")
