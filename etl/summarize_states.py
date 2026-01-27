import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"

df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

# --- Basic cleaning ---
if "county_fips" in df.columns:
    df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

if "data_status" not in df.columns:
    df["data_status"] = "VALID"

df["data_status"] = (
    df["data_status"].fillna("VALID").astype(str).str.strip().str.upper()
)

# numeric coercion helper (handles commas/%)
def coerce_numeric(col: str) -> None:
    if col not in df.columns:
        return
    if pd.api.types.is_numeric_dtype(df[col]):
        return
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    df[col] = pd.to_numeric(df[col], errors="coerce")

for col in [
    "care_gap_index",
    "population",
    "hospitals_reporting",
    "stroke_mortality_rate",
    "uninsured_rate",
    "burden_index",
    "supply_score",
    "SCAI",
]:
    coerce_numeric(col)

# normalize uninsured rate if it looks like fraction
if "uninsured_rate" in df.columns:
    med_unins = df["uninsured_rate"].median(skipna=True)
    if pd.notna(med_unins) and med_unins <= 1.0:
        df["uninsured_rate"] = df["uninsured_rate"] * 100.0

# If SCAI is missing, compute it from care_gap_index like your app
if "SCAI" not in df.columns or df["SCAI"].isna().all():
    if "care_gap_index" in df.columns:
        care = pd.to_numeric(df["care_gap_index"], errors="coerce")
        mn, mx = care.min(), care.max()
        if pd.notna(mn) and pd.notna(mx) and mx > mn:
            df["SCAI"] = 1.0 - (care - mn) / (mx - mn)
        else:
            df["SCAI"] = np.nan
    else:
        df["SCAI"] = np.nan

# --- Valid counties (match your "not insufficient" logic) ---
is_valid = ~df["data_status"].str.contains("INSUFFICIENT", na=False)

valid = df[
    is_valid
    & df["state"].notna()
    & df["SCAI"].notna()
    & df["population"].notna()
].copy()

# Optional: keep your population threshold if you truly want it
# valid = valid[valid["population"] >= 10000].copy()

# --- State summaries ---
def weighted_mean(x, w):
    x = np.asarray(x, dtype=float)
    w = np.asarray(w, dtype=float)
    mask = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if mask.sum() == 0:
        return np.nan
    return np.average(x[mask], weights=w[mask])

grouped = (
    valid.groupby("state", as_index=False)
    .agg(
        n_counties=("county_fips", "count"),
        total_population=("population", "sum"),
        unweighted_mean_SCAI=("SCAI", "mean"),
        total_hospitals=("hospitals_reporting", "sum"),
    )
)

# population-weighted mean SCAI
grouped["pop_weighted_mean_SCAI"] = (
    valid.groupby("state")
    .apply(lambda g: weighted_mean(g["SCAI"], g["population"]))
    .reset_index(drop=True)
)

# hospitals per 100k
grouped["hospitals_per_100k"] = (
    grouped["total_hospitals"] / grouped["total_population"] * 100000
)

# Rankings (1 = best access)
grouped["rank_unweighted"] = grouped["unweighted_mean_SCAI"].rank(
    method="dense", ascending=False
)
grouped["rank_pop_weighted"] = grouped["pop_weighted_mean_SCAI"].rank(
    method="dense", ascending=False
)

# Sort by weighted ranking by default (you can change)
grouped = grouped.sort_values(["rank_pop_weighted", "rank_unweighted"])

out_path = CLEAN / "state_SCAI_rankings_unweighted_vs_popweighted.csv"
grouped.to_csv(out_path, index=False)

print(f"✔️ State rankings saved → {out_path}")

out_xlsx = CLEAN / "state_care_gap_summary.xlsx"
grouped.to_excel(out_xlsx, index=False)
print(f"✔️ Excel saved → {out_xlsx}")
