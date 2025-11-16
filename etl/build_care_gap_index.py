import pandas as pd
import numpy as np
from pathlib import Path

CLEAN = Path("data_clean")

# ---- Load inputs ----
county = pd.read_csv(CLEAN / "county_profile.csv", dtype=str)
hosp = pd.read_csv(CLEAN / "hospital_profile.csv", dtype=str)

# ---- Ensure numeric county fields ----
county["stroke_mortality_rate"] = pd.to_numeric(
    county["stroke_mortality_rate"], errors="coerce"
)
county["uninsured_rate"] = pd.to_numeric(
    county["uninsured_rate"].astype(str).str.replace("%", "", regex=False),
    errors="coerce"
)
county["burden_index"] = pd.to_numeric(county["burden_index"], errors="coerce")

# Drop rows with invalid or extreme mortality values
county = county[
    county["stroke_mortality_rate"].notna()
    & (county["stroke_mortality_rate"] > 0)
    & (county["stroke_mortality_rate"] < 200)
].copy()

# ---- Clean hospital stroke scores ----
hosp["stroke_score_num"] = pd.to_numeric(hosp["stroke_score"], errors="coerce")

# Map performance labels to weights
perf_map = {
    "Better Than the National Rate": 2,
    "No Different Than the National Rate": 1,
    "Worse Than the National Rate": 0,
}
hosp["performance_weight"] = hosp["compared_to_national"].map(perf_map).astype(float)

# Penalize missing / Not Available reporting
mask_na_score = hosp["stroke_score_num"].isna()
mask_not_avail = hosp["stroke_score"].astype(str).str.contains(
    "Not Available", case=False, na=False
)
hosp.loc[mask_na_score | mask_not_avail, "performance_weight"] = -1.0

# ---- Normalize state + county names on BOTH tables ----
def normalize_name(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.upper()
    # strip suffixes like " COUNTY", " PARISH", " BOROUGH"
    s = s.str.replace(" COUNTY", "", regex=False)
    s = s.str.replace(" PARISH", "", regex=False)
    s = s.str.replace(" BOROUGH", "", regex=False)
    return s

# add normalized columns
county["state_norm"] = normalize_name(county["state"])
county["county_norm"] = normalize_name(county["county_name"])

hosp["state_norm"] = normalize_name(hosp["state"])
hosp["county_norm"] = normalize_name(hosp["county"])

# ---- Hospital summary by (state, county) ----
hospital_summary = (
    hosp.groupby(["state_norm", "county_norm"], as_index=False)
    .agg(
        hospitals_reporting=("stroke_score_num", "count"),
        mean_stroke_score=("stroke_score_num", "mean"),
        mean_performance_weight=("performance_weight", "mean"),
    )
)

# ---- Merge summary into county_profile ----
merged = county.merge(
    hospital_summary,
    on=["state_norm", "county_norm"],
    how="left",
)

# If a county has no matching hospital, treat as zero supply
merged["hospitals_reporting"] = merged["hospitals_reporting"].fillna(0)
# for performance weight, "no hospitals" or missing → treat as -1 (penalized / unknown)
merged["mean_performance_weight"] = merged["mean_performance_weight"].fillna(-1.0)

# ---- Compute burden_scaled and supply_score ----
merged["burden_scaled"] = np.log1p(merged["burden_index"])

# Only positive performance contributes to supply; negative/NA means no effective supply
effective_supply = (
    merged["hospitals_reporting"]
    * merged["mean_performance_weight"].clip(lower=0)
)
merged["supply_score"] = np.log1p(effective_supply)

# ---- Final care_gap_index (revised formula) ----
max_supply = merged["supply_score"].max()
if pd.isna(max_supply) or max_supply <= 0:
    # fallback: purely burden-driven
    merged["care_gap_index_raw"] = merged["burden_scaled"]
else:
    merged["care_gap_index_raw"] = merged["burden_scaled"] * (
        1 - (merged["supply_score"] / max_supply)
    )

# Normalize care_gap_index_raw to 0–1
min_val = merged["care_gap_index_raw"].min()
max_val2 = merged["care_gap_index_raw"].max()
if pd.isna(min_val) or pd.isna(max_val2) or max_val2 == min_val:
    merged["care_gap_index"] = 0.0
else:
    merged["care_gap_index"] = (merged["care_gap_index_raw"] - min_val) / (
        max_val2 - min_val
    )

# ---- Rank + categorize ----
merged["national_rank"] = merged["care_gap_index"].rank(
    method="dense", ascending=False
)

percentiles = merged["national_rank"] / merged["national_rank"].max()

merged["category"] = pd.cut(
    percentiles,
    bins=[0, 0.1, 0.3, 0.6, 0.9, 1.0],
    labels=[
        "Critical Gap",
        "High Gap",
        "Moderate Gap",
        "Low Gap",
        "Best-Aligned Care",
    ],
)

# ---- Save care gap index ----
merged.to_csv(CLEAN / "stroke_care_gap_index.csv", index=False)

# ---- Hospital benchmarking: keep as before ----
hosp["national_percentile"] = hosp["stroke_score_num"].rank(pct=True)
hosp.to_csv(CLEAN / "hospital_benchmarking.csv", index=False)

print("Generated:")
print(" - stroke_care_gap_index.csv")
print(" - hospital_benchmarking.csv")
