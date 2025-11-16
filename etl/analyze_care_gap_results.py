import pandas as pd
from pathlib import Path

CLEAN = Path("data_clean")

# ---- Load the full care gap index ----
df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

# Make sure numeric fields are numeric
df["population"] = pd.to_numeric(df.get("population"), errors="coerce")
df["care_gap_index"] = pd.to_numeric(df.get("care_gap_index"), errors="coerce")
df["hospitals_reporting"] = pd.to_numeric(df.get("hospitals_reporting"), errors="coerce")

# ---- Define which rows are actually valid for ranking ----
valid_mask = (
    (df["data_status"] == "VALID")      # from build_care_gap_index.py
    & df["care_gap_index"].notna()
    & df["population"].notna()
    & (df["population"] >= 1000)        # hard cutoff: tiny counties not ranked
)

valid = df[valid_mask].copy()

# Safety: if national_rank isn't already there or changed upstream, re-rank here
valid["national_rank"] = valid["care_gap_index"].rank(method="dense", ascending=False)

# ---- Best 50 (lowest care_gap_index = best aligned) ----
best_50 = (
    valid.sort_values("care_gap_index", ascending=True)
    .head(50)
    .copy()
)

# ---- Worst 50 (highest care_gap_index = biggest gap) ----
worst_50 = (
    valid.sort_values("care_gap_index", ascending=False)
    .head(50)
    .copy()
)

# ---- Choose columns to export ----
cols = [
    "county_fips",
    "county_name",
    "state",
    "population",
    "hospitals_reporting",
    "stroke_mortality_rate",
    "uninsured_rate",
    "care_gap_index",
    "data_status",
    "category",
    "national_rank",
]

best_50.to_csv(CLEAN / "stroke_care_gap_best_50.csv", index=False, columns=[c for c in cols if c in best_50.columns])
worst_50.to_csv(CLEAN / "stroke_care_gap_worst_50.csv", index=False, columns=[c for c in cols if c in worst_50.columns])

print("Generated:")
print(" - stroke_care_gap_best_50.csv")
print(" - stroke_care_gap_worst_50.csv")
print("Valid counties used for ranking:", len(valid))
