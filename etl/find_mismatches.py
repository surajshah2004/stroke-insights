import pandas as pd
import numpy as np
from pathlib import Path

CLEAN = Path("data_clean")

# Load care gap index table
df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype=str)

# Numeric conversions
df["stroke_mortality_rate"] = pd.to_numeric(
    df["stroke_mortality_rate"], errors="coerce"
)
df["care_gap_index"] = pd.to_numeric(
    df.get("care_gap_index", np.nan), errors="coerce"
)

# Use only rows with a valid mortality rate
df = df[df["stroke_mortality_rate"].notna()].copy()

# If there is a data_status column, use only VALID rows
if "data_status" in df.columns:
    df_valid = df[df["data_status"] == "VALID"].copy()
else:
    df_valid = df.copy()

# Drop rows with missing category
df_valid = df_valid[df_valid["category"].notna()].copy()

# Compute mortality percentiles
mort = df_valid["stroke_mortality_rate"]
hi_thresh = mort.quantile(0.90)  # top 10 percent mortality
lo_thresh = mort.quantile(0.10)  # bottom 10 percent mortality

print(f"High mortality threshold (90th percentile): {hi_thresh:.1f}")
print(f"Low mortality threshold  (10th percentile): {lo_thresh:.1f}")

# 1) High mortality but labeled Best-Aligned Care
high_mort_best = df_valid[
    (df_valid["stroke_mortality_rate"] >= hi_thresh)
    & (df_valid["category"] == "Best-Aligned Care")
].copy()

high_mort_best = high_mort_best.sort_values(
    "stroke_mortality_rate", ascending=False
)

# 2) Low mortality but labeled Critical Gap
low_mort_critical = df_valid[
    (df_valid["stroke_mortality_rate"] <= lo_thresh)
    & (df_valid["category"] == "Critical Gap")
].copy()

low_mort_critical = low_mort_critical.sort_values(
    "stroke_mortality_rate", ascending=True
)

# Select useful columns for review
cols_for_review = [
    "county_fips",
    "county_name",
    "state",
    "population",
    "stroke_mortality_rate",
    "uninsured_rate",
    "hospitals_reporting",
    "mean_performance_weight",
    "care_gap_index",
    "category",
    "data_status",
]

cols_for_review = [c for c in cols_for_review if c in df_valid.columns]

high_mort_best_out = high_mort_best[cols_for_review]
low_mort_critical_out = low_mort_critical[cols_for_review]

# Write outputs
out1 = CLEAN / "stroke_mismatch_high_mort_best_label.csv"
out2 = CLEAN / "stroke_mismatch_low_mort_critical_label.csv"

high_mort_best_out.to_csv(out1, index=False)
low_mort_critical_out.to_csv(out2, index=False)

print()
print(f"High mortality + Best-Aligned Care mismatches: {len(high_mort_best_out)}")
print(f" -> wrote {out1}")
print()
print(f"Low mortality + Critical Gap mismatches: {len(low_mort_critical_out)}")
print(f" -> wrote {out2}")

# Show first few rows for quick sanity check
if len(high_mort_best_out) > 0:
    print("\nExample high-mortality / Best-Aligned rows:")
    print(high_mort_best_out.head(10))

if len(low_mort_critical_out) > 0:
    print("\nExample low-mortality / Critical Gap rows:")
    print(low_mort_critical_out.head(10))
