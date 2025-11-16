import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"

df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

# Numeric conversions
for col in ["care_gap_index", "population", "hospitals_reporting",
            "stroke_mortality_rate", "uninsured_rate"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Valid counties only (same logic as map)
if "data_status" not in df.columns:
    df["data_status"] = "VALID"

valid = df[
    (df["data_status"] == "VALID")
    & df["care_gap_index"].notna()
    & df["population"].notna()
    & (df["population"] >= 10000)
].copy()

# Group by state
grouped = (
    valid.groupby("state", as_index=False)
    .agg(
        n_counties=("county_fips", "count"),
        total_population=("population", "sum"),
        mean_care_gap_index=("care_gap_index", "mean"),
        mean_stroke_mortality=("stroke_mortality_rate", "mean"),
        mean_uninsured_rate=("uninsured_rate", "mean"),
        total_hospitals=("hospitals_reporting", "sum"),
    )
)

# Hospitals per 100k population
grouped["hospitals_per_100k"] = (
    grouped["total_hospitals"] / grouped["total_population"] * 100000
)

# Rank states by care gap index (higher = worse)
grouped["state_rank"] = grouped["mean_care_gap_index"].rank(
    method="dense", ascending=False
)

grouped = grouped.sort_values("mean_care_gap_index", ascending=False)

out_path = CLEAN / "state_care_gap_summary.csv"
grouped.to_csv(out_path, index=False)

print(f"✔️ State summary saved → {out_path}")
