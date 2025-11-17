import pandas as pd
from pathlib import Path

CLEAN = Path("data_clean")
output_dir = CLEAN  # all outputs live in data_clean

# ---- Load the full county index ----
df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

# ---- Make sure numeric fields are numeric ----
df["population"] = pd.to_numeric(df.get("population"), errors="coerce")
df["care_gap_index"] = pd.to_numeric(df.get("care_gap_index"), errors="coerce")
df["hospitals_reporting"] = pd.to_numeric(df.get("hospitals_reporting"), errors="coerce")
df["SCAI"] = pd.to_numeric(df.get("SCAI"), errors="coerce")

# ---- Define which rows are actually valid for ranking ----
valid_mask = (
    (df["data_status"] == "VALID")      # from build_care_gap_index.py
    & df["SCAI"].notna()                # must have a valid SCAI score
    & df["population"].notna()
    & (df["population"] >= 1000)        # hard cutoff: tiny counties not ranked
)

valid_df = df[valid_mask].copy()

# ---- Top 50 (highest access = highest SCAI) ----
best50 = (
    valid_df.sort_values("SCAI", ascending=False)
    .head(50)
    .copy()
)

# ---- Bottom 50 (lowest access = lowest SCAI) ----
worst50 = (
    valid_df.sort_values("SCAI", ascending=True)
    .head(50)
    .copy()
)

# ---- Save SCAI-based best/worst lists ----
output_dir.mkdir(exist_ok=True, parents=True)

best50.to_csv(output_dir / "stroke_care_access_best_50.csv", index=False)
worst50.to_csv(output_dir / "stroke_care_access_worst_50.csv", index=False)

print("\nSaved:")
print(" - stroke_care_access_best_50.csv")
print(" - stroke_care_access_worst_50.csv")

# ---- Also export gap-style best/worst for compatibility ----
cols = [
    "county_fips",
    "county_name",
    "state",
    "population",
    "hospitals_reporting",
    "stroke_mortality_rate",
    "uninsured_rate",
    "care_gap_index",
    "SCAI",
    "data_status",
    "category",
    "national_rank",
]

best50_subset = best50[[c for c in cols if c in best50.columns]]
worst50_subset = worst50[[c for c in cols if c in worst50.columns]]

best50_subset.to_csv(
    CLEAN / "stroke_care_gap_best_50.csv",
    index=False,
)
worst50_subset.to_csv(
    CLEAN / "stroke_care_gap_worst_50.csv",
    index=False,
)

print("Generated:")
print(" - stroke_care_gap_best_50.csv")
print(" - stroke_care_gap_worst_50.csv")
print("Valid counties used for ranking:", len(valid_df))
