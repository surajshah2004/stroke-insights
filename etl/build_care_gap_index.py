import pandas as pd
import numpy as np
from pathlib import Path

CLEAN = Path("data_clean")

county = pd.read_csv(CLEAN / "county_profile.csv", dtype=str)
hosp = pd.read_csv(CLEAN / "hospital_profile.csv", dtype=str)

# Ensure numeric
county["stroke_mortality_rate"] = pd.to_numeric(county["stroke_mortality_rate"], errors="coerce")
county["uninsured_rate"] = pd.to_numeric(county["uninsured_rate"].str.replace("%","",regex=False), errors="coerce")
county["burden_index"] = pd.to_numeric(county["burden_index"], errors="coerce")

# clean hospital scores
hosp["stroke_score_num"] = pd.to_numeric(hosp["stroke_score"], errors="coerce")

# performance weighting
perf_map = {
    "Better Than the National Rate": 2,
    "No Different Than the National Rate": 1,
    "Worse Than the National Rate": 0
}

hosp["performance_weight"] = hosp["compared_to_national"].map(perf_map)

# group by county
hospital_summary = hosp.groupby("county", as_index=False).agg(
    hospitals_reporting=("stroke_score_num", "count"),
    mean_stroke_score=("stroke_score_num", "mean"),
    mean_performance_weight=("performance_weight", "mean")
)

# merge with county profile
merged = county.merge(
    hospital_summary,
    left_on="county_name",
    right_on="county",
    how="left"
)

# fill missing hospitals with zero presence
merged["hospitals_reporting"] = merged["hospitals_reporting"].fillna(0)
merged["mean_performance_weight"] = merged["mean_performance_weight"].fillna(0)

# log scaling
merged["burden_scaled"] = np.log1p(merged["burden_index"])
merged["supply_score"] = np.log1p(merged["hospitals_reporting"] * merged["mean_performance_weight"])

# final score
merged["care_gap_index"] = merged["burden_scaled"] / (1 + merged["supply_score"])

# ranking and bins
merged["national_rank"] = merged["care_gap_index"].rank(method="dense", ascending=False)

percentiles = merged["national_rank"] / merged["national_rank"].max()

merged["category"] = pd.cut(
    percentiles,
    bins=[0,0.1,0.3,0.6,0.9,1],
    labels=["Critical Gap", "High Gap", "Moderate Gap", "Low Gap", "Best-Aligned Care"]
)

# write output
merged.to_csv(CLEAN / "stroke_care_gap_index.csv", index=False)


# hospital benchmarking output
hosp["national_percentile"] = hosp["stroke_score_num"].rank(pct=True)
hosp.to_csv(CLEAN / "hospital_benchmarking.csv", index=False)

print("Generated:")
print(" - stroke_care_gap_index.csv")
print(" - hospital_benchmarking.csv")
