import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"

@st.cache_data
def load_county_data():
    df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

    # Ensure FIPS are 5-digit strings
    df["county_fips"] = df["county_fips"].str.zfill(5)

    # Numeric fields
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

    # Data status & display category
    if "data_status" not in df.columns:
        df["data_status"] = "VALID"

    low_pop = df["population"] < 5000
    no_hosp = df["hospitals_reporting"].fillna(0) == 0
    not_valid = df["data_status"] != "VALID"
    insufficient_mask = (low_pop & no_hosp) | not_valid

    # Use your existing category, but override low-data counties
    df["display_category"] = df.get("category", "No Data")
    df.loc[insufficient_mask, "display_category"] = "No / Insufficient Data"

    return df

# ---- Load data ----
df = load_county_data()

# ---- Page config ----
st.set_page_config(page_title="Stroke Care Gap Explorer", layout="wide")
st.title("U.S. Stroke Care Gap Explorer")
st.caption(
    "County-level Stroke Care Gap Index combining stroke mortality, uninsured rate, "
    "hospital supply, and performance."
)

# ---- Sidebar filters ----
st.sidebar.header("Filters")

# State filter
states = sorted(df["state"].dropna().unique())
state_filter = st.sidebar.multiselect(
    "State(s)",
    options=states,
    default=None,
    help="Limit the map and table to selected states.",
)

# Category filter
categories = [
    "Critical Gap",
    "High Gap",
    "Moderate Gap",
    "Low Gap",
    "Best-Aligned Care",
    "No / Insufficient Data",
]
category_filter = st.sidebar.multiselect(
    "Category",
    options=categories,
    default=categories,  # show all categories by default
)

# Population filter
pop_min = int(df["population"].min())
pop_max = int(df["population"].max())
pop_range = st.sidebar.slider(
    "Population range",
    min_value=0,
    max_value=pop_max,
    value=(0, pop_max),
    step=1000,
)

# ---- Apply filters ----
filtered = df.copy()

if state_filter:
    filtered = filtered[filtered["state"].isin(state_filter)]

if category_filter:
    filtered = filtered[filtered["display_category"].isin(category_filter)]

filtered = filtered[
    (filtered["population"] >= pop_range[0])
    & (filtered["population"] <= pop_range[1])
]

st.sidebar.write(f"Showing **{len(filtered)}** counties")

# ---- Categorical map ----
geojson_url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

color_map = {
    "Critical Gap": "#8e0000",
    "High Gap": "#d64545",
    "Moderate Gap": "#eabf3f",
    "Low Gap": "#7cc467",
    "Best-Aligned Care": "#2e7d32",
    "No / Insufficient Data": "#cccccc",
}

fig = px.choropleth(
    filtered,
    geojson=geojson_url,
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
        "data_status": True,
        "county_fips": False,
    },
    labels={"display_category": "Stroke Care Category"},
)

fig.update_layout(
    margin={"r": 0, "t": 30, "l": 0, "b": 0},
)

st.plotly_chart(fig, use_container_width=True)

# ---- County table ----
st.subheader("County-level details")

cols_to_show = [
    "state",
    "county_name",
    "population",
    "care_gap_index",
    "display_category",
    "stroke_mortality_rate",
    "uninsured_rate",
    "hospitals_reporting",
    "national_rank",
]
cols_to_show = [c for c in cols_to_show if c in filtered.columns]

st.dataframe(
    filtered[cols_to_show].sort_values("care_gap_index", ascending=False),
    use_container_width=True,
    hide_index=True,
)
