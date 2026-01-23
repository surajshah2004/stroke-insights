from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# Paths
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"

# Display names used in Plotly labels and table renames
DISPLAY_NAMES = {
    "stroke_mortality_rate": "Stroke mortality (deaths per 100,000)",
    "uninsured_rate": "Uninsured rate (%)",
    "hospitals_reporting": "Hospitals reporting",
    "care_gap_index": "Care gap index",
    "SCAI": "SCAI",
    "access_rank": "Access rank (1 = best)",
    "burden_index": "Burden index",
    "supply_score": "Supply score",
    "display_category": "Stroke care category",
    "data_status": "Data status",
}

@st.cache_data
def load_county_data() -> pd.DataFrame:
    df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

    # Ensure FIPS are 5-digit strings
    df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

    # Make key fields numeric (only if present)
    numeric_cols = [
        "care_gap_index",
        "population",
        "hospitals_reporting",
        "stroke_mortality_rate",
        "uninsured_rate",
        "burden_index",
        "supply_score",
        "SCAI",
        "access_rank",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Default data_status
    if "data_status" not in df.columns:
        df["data_status"] = "VALID"

    # Normalize uninsured_rate to percent if it looks like a fraction
    if "uninsured_rate" in df.columns:
        m = df["uninsured_rate"].median(skipna=True)
        if pd.notna(m) and m <= 1.0:
            df["uninsured_rate"] = df["uninsured_rate"] * 100.0

    # Compute Burden Index if missing
    # burden_index = log1p(stroke_mortality_rate * uninsured_rate_fraction)
    if "burden_index" not in df.columns:
        if {"stroke_mortality_rate", "uninsured_rate"} <= set(df.columns):
            prod = df["stroke_mortality_rate"] * (df["uninsured_rate"] / 100.0)
            df["burden_index"] = np.log1p(prod)

    # Compute SCAI if missing
    # If you already compute SCAI in ETL, it will be used as-is
    if "SCAI" not in df.columns:
        care = pd.to_numeric(df["care_gap_index"], errors="coerce")
        min_val = care.min()
        max_val = care.max()
        if pd.notna(min_val) and pd.notna(max_val) and max_val > min_val:
            care_norm = (care - min_val) / (max_val - min_val)
            df["SCAI"] = 1.0 - care_norm
        else:
            df["SCAI"] = np.nan

    # access_rank: 1 = best access
    df["access_rank"] = df["SCAI"].rank(method="dense", ascending=False)

    # Build categories from SCAI quantiles
    df["display_category"] = "No / Insufficient Data"

    valid_mask = (
        (df["data_status"] == "VALID")
        & df["care_gap_index"].notna()
        & df["SCAI"].notna()
    )
    valid = df[valid_mask].copy()

    if len(valid) > 0:
        valid["display_category"] = pd.qcut(
            valid["SCAI"],
            q=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
            labels=[
                "Critical Gap",
                "High Gap",
                "Moderate Gap",
                "Low Gap",
                "Best-Aligned Care",
            ],
            duplicates="drop",
        ).astype(str)
        df.loc[valid.index, "display_category"] = valid["display_category"]

    return df


# Page config
st.set_page_config(page_title="Stroke Care Access Explorer", layout="wide")
st.title("U.S. Stroke Care Access Explorer")
st.caption(
    "County-level Stroke Care Access Index (SCAI) combining stroke mortality (deaths per 100,000), "
    "uninsured rate, hospital supply, and performance."
)

# Load data
df = load_county_data()

# Sidebar filters
st.sidebar.header("Filters")

states = sorted(df["state"].dropna().unique())
state_filter = st.sidebar.multiselect(
    "State(s)",
    options=states,
    default=None,
    help="Limit the map and table to selected states.",
)

available_categories = sorted(df["display_category"].dropna().unique().tolist())
category_filter = st.sidebar.multiselect(
    "Category",
    options=available_categories,
    default=available_categories,
)

pop_max = int(df["population"].max(skipna=True)) if "population" in df.columns else 0
pop_range = st.sidebar.slider(
    "Population range",
    min_value=0,
    max_value=pop_max,
    value=(0, pop_max),
    step=1000,
)

# Apply filters
filtered = df.copy()

if state_filter:
    filtered = filtered[filtered["state"].isin(state_filter)]

if category_filter:
    filtered = filtered[filtered["display_category"].isin(category_filter)]

# Keep rows with missing population. Filter only those with known pop.
if "population" in filtered.columns:
    pop_mask = (
        filtered["population"].isna()
        | (
            (filtered["population"] >= pop_range[0])
            & (filtered["population"] <= pop_range[1])
        )
    )
    filtered = filtered[pop_mask]

st.sidebar.write(f"Showing **{len(filtered)}** counties")


# Population by care gap category
st.subheader("Population by care gap category")

if "population" in filtered.columns:
    pop_summary = (
        filtered.dropna(subset=["population"])
        .groupby("display_category", observed=True)["population"]
        .sum()
        .reset_index()
        .rename(columns={"population": "population"})
    )

    total_pop = pop_summary["population"].sum()
    if total_pop and total_pop > 0:
        pop_summary["share_of_population_pct"] = (pop_summary["population"] / total_pop) * 100.0
    else:
        pop_summary["share_of_population_pct"] = np.nan

    pop_summary = pop_summary.sort_values("population", ascending=False)
    pop_summary["population"] = pop_summary["population"].round(0).astype("Int64")
    pop_summary["share_of_population_pct"] = pop_summary["share_of_population_pct"].round(2)

    pop_display = pop_summary.rename(
        columns={
            "display_category": "Category",
            "population": "Population",
            "share_of_population_pct": "Share of population (%)",
        }
    )

    st.dataframe(pop_display, use_container_width=True, hide_index=True)
    st.caption("Shares are computed among counties with non-missing population in the current filter view.")
else:
    st.info("Population column not found in the dataset.")


# Categorical map
geojson_url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

ordered_categories = [
    "Best-Aligned Care",
    "Low Gap",
    "Moderate Gap",
    "High Gap",
    "Critical Gap",
    "No / Insufficient Data",
]

filtered["display_category"] = pd.Categorical(
    filtered["display_category"],
    categories=ordered_categories,
    ordered=True,
)

color_map = {
    "Best-Aligned Care": "#1B5E20",
    "Low Gap": "#81C784",
    "Moderate Gap": "#FFEB3B",
    "High Gap": "#FF8A65",
    "Critical Gap": "#B71C1C",
    "No / Insufficient Data": "#BDBDBD",
}

# Hover formatting map. Add burden_index and supply_score if present.
hover_data = {
    "state": True,
    "population": ":,",
    "hospitals_reporting": True,
    "stroke_mortality_rate": ":.1f",
    "uninsured_rate": ":.1f",
    "care_gap_index": ":.3f",
    "SCAI": ":.3f",
    "access_rank": ":.0f",
    "display_category": True,
    "data_status": True,
    "county_fips": False,
}
if "burden_index" in filtered.columns:
    hover_data["burden_index"] = ":.3f"
if "supply_score" in filtered.columns:
    hover_data["supply_score"] = ":.3f"

labels = {"display_category": DISPLAY_NAMES["display_category"]}
for k, v in DISPLAY_NAMES.items():
    labels[k] = v

fig = px.choropleth(
    filtered,
    geojson=geojson_url,
    locations="county_fips",
    color="display_category",
    color_discrete_map=color_map,
    scope="usa",
    hover_name="county_name",
    hover_data=hover_data,
    labels=labels,
)

fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0})
st.plotly_chart(fig, use_container_width=True)


# County table
st.subheader("County-level details")

cols_to_show = [
    "state",
    "county_name",
    "population",
    "SCAI",
    "access_rank",
    "display_category",
    "stroke_mortality_rate",
    "uninsured_rate",
    "hospitals_reporting",
    "burden_index",
    "supply_score",
    "care_gap_index",
    "data_status",
]
cols_to_show = [c for c in cols_to_show if c in filtered.columns]

table_df = filtered[cols_to_show].copy()
table_df = table_df.rename(columns=DISPLAY_NAMES)

sort_col = DISPLAY_NAMES.get("access_rank", "access_rank")
if sort_col in table_df.columns:
    table_df = table_df.sort_values(sort_col, ascending=True)

st.dataframe(table_df, use_container_width=True, hide_index=True)
