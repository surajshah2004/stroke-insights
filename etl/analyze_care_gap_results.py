import pandas as pd
from pathlib import Path

CLEAN = Path("data_clean")

df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype=str)

# convert numeric columns
num_cols = ["care_gap_index", "hospitals_reporting", "stroke_mortality_rate", "uninsured_rate"]
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# ---- TOP 50 WORST (highest care gap index) ----
worst50 = df.nlargest(50, "care_gap_index")[
    ["county_name", "state", "care_gap_index", "hospitals_reporting", "stroke_mortality_rate", "uninsured_rate", "category"]
]

worst50.to_csv(CLEAN / "worst_50_care_gap_counties.csv", index=False)


# ---- TOP 50 BEST (lowest care gap index) ----
best50 = df.nsmallest(50, "care_gap_index")[
    ["county_name", "state", "care_gap_index", "hospitals_reporting", "stroke_mortality_rate", "uninsured_rate", "category"]
]

best50.to_csv(CLEAN / "best_50_care_gap_counties.csv", index=False)


print("\nGenerated reports:")
print(" - worst_50_care_gap_counties.csv")
print(" - best_50_care_gap_counties.csv")
