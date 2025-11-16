import pandas as pd
import plotly.express as px
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"

# --------------------------------------------------
# Load county-level stroke care gap index
# --------------------------------------------------
df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

# Ensure 5-digit FIPS
df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

# Make sure key numeric fields are numeric
numeric_cols = [
    "care_gap_index",
    "population",
    "hospitals_reporting",
    "stroke_mortality_rate",
    "uninsured_rate",
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Derive display category if missing
if "category" not in df.columns:
    df["category"] = "Unknown"

# If data_status exists, mark insufficient-data counties
if "data_status" in df.columns:
    low_pop = df["population"] < 5000
    no_hosp = df["hospitals_reporting"].fillna(0) == 0
    not_valid = df["data_status"] != "VALID"
    insufficient_mask = (low_pop & no_hosp) | not_valid
else:
    insufficient_mask = pd.Series(False, index=df.index)

df["display_category"] = df["category"]
df.loc[insufficient_mask, "display_category"] = "No / Insufficient Data"

# --------------------------------------------------
# GeoJSON for US counties
# --------------------------------------------------
GEOJSON_URL = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

# --------------------------------------------------
# 1) Continuous care_gap_index heatmap
# --------------------------------------------------
fig_cont = px.choropleth(
    df,
    geojson=GEOJSON_URL,
    locations="county_fips",
    color="care_gap_index",
    color_continuous_scale="YlOrRd",
    scope="usa",
    hover_name="county_name",
    hover_data={
        "state": True,
        "population": True,
        "hospitals_reporting": True,
        "stroke_mortality_rate": True,
        "uninsured_rate": True,
        "care_gap_index": True,
        "category": True,
        "display_category": True,
        "county_fips": False,
    },
    labels={"care_gap_index": "Stroke Care Gap Index"},
)

fig_cont.update_layout(
    title_text="Stroke Care Gap Index by County (Continuous)",
    margin={"r": 0, "t": 40, "l": 0, "b": 0},
)

out_cont = CLEAN / "stroke_care_gap_map_continuous.html"
fig_cont.write_html(out_cont)
print(f"✔️ Wrote {out_cont}")

# --------------------------------------------------
# 2) Categorical heatmap by display_category
# --------------------------------------------------
color_map = {
    "Critical Gap": "#8e0000",
    "High Gap": "#d64545",
    "Moderate Gap": "#eabf3f",
    "Low Gap": "#7cc467",
    "Best-Aligned Care": "#2e7d32",
    "No / Insufficient Data": "#cccccc",
}

fig_cat = px.choropleth(
    df,
    geojson=GEOJSON_URL,
    locations="county_fips",
    color="display_category",
    color_discrete_map=color_map,
    scope="usa",
    hover_name="county_name",
    hover_data={
        "state": True,
        "population": True,
        "hospitals_reporting": True,
        "stroke_mortality_rate": True,
        "uninsured_rate": True,
        "care_gap_index": True,
        "display_category": True,
        "data_status": df.get("data_status", None) is not None,
        "county_fips": False,
    },
    labels={"display_category": "Stroke Care Category"},
)

fig_cat.update_layout(
    title_text="Stroke Care Gap Index by County (Categorical)",
    margin={"r": 0, "t": 40, "l": 0, "b": 0},
)

out_cat = CLEAN / "stroke_care_gap_map_categories.html"
fig_cat.write_html(out_cat)
print(f"✔️ Wrote {out_cat}")

print("✅ Heatmap build complete.")
