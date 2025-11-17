from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

# Paths
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"

@st.cache_data
def load_county_data():
    df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

    # Ensure FIPS are 5-digit strings
    df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

    # Make key fields numeric
    for col in ["care_gap_index", "population", "hospitals_reporting",
                "stroke_mortality_rate", "uninsured_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Default data_status
    if "data_status" not in df.columns:
        df["data_status"] = "VALID"

    # ---- Compute SCAI (0–1, higher = better access) ----
    if "SCAI" not in df.columns:
        care = pd.to_numeric(df["care_gap_index"], errors="coerce")
        min_val = care.min()
        max_val = care.max()
        if pd.notna(min_val) and pd.notna(max_val) and max_val > min_val:
            care_norm = (care - min_val) / (max_val - min_val)
            df["SCAI"] = 1.0 - care_norm
        else:
            df["SCAI"] = 0.0

    # ---- access_rank: 1 = best access ----
    df["access_rank"] = df["SCAI"].rank(method="dense", ascending=False)

    # ---- Build categories from SCAI quantiles ----
    df["display_category"] = "No / Insufficient Data"

    valid_mask = (
        (df["data_status"] == "VALID") &
        df["care_gap_index"].notna() &
        df["SCAI"].notna()
    )
    valid = df[valid_mask].copy()

    if len(valid) > 0:
        # 5 bins: worst -> best, but labels reflect gap (best = least gap)
        valid["display_category"] = pd.qcut(
            valid["SCAI"],
            q=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
            labels=[
                "Critical Gap",      # lowest SCAI (worst access)
                "High Gap",
                "Moderate Gap",
                "Low Gap",
                "Best-Aligned Care" # highest SCAI (best access)
            ],
            duplicates="drop",
        ).astype(str)

        # Write back into main df
        df.loc[valid.index, "display_category"] = valid["display_category"]

    return df

# ---- Load data ----
df = load_county_data()


# ---- Derive SCAI + access_rank (1 = BEST access) ----
# If SCAI isn't already in the CSV, construct it as 1 - normalized care_gap_index
if "SCAI" not in df.columns:
    care = pd.to_numeric(df["care_gap_index"], errors="coerce")
    min_val = care.min()
    max_val = care.max()
    if pd.notna(min_val) and pd.notna(max_val) and max_val > min_val:
        care_norm = (care - min_val) / (max_val - min_val)
        df["SCAI"] = 1.0 - care_norm      # higher SCAI = better access
    else:
        df["SCAI"] = 0.0

# Rank so that 1 = highest SCAI (best access)
df["access_rank"] = df["SCAI"].rank(method="dense", ascending=False)

# ---- Page config ----
st.set_page_config(page_title="Stroke Care Access Explorer", layout="wide")
st.title("U.S. Stroke Care Access Explorer")
st.caption(
    "County-level Stroke Care Access Index (SCAI) combining stroke mortality, uninsured rate, "
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

# Category filter – derive from display_category
available_categories = sorted(df["display_category"].dropna().unique().tolist())
category_filter = st.sidebar.multiselect(
    "Category",
    options=available_categories,
    default=available_categories,
)

# Population slider (only based on known population)
pop_min = int(df["population"].min(skipna=True))
pop_max = int(df["population"].max(skipna=True))
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

# Keep rows with missing population; filter only those with known pop
pop_mask = (
    filtered["population"].isna()
    | (
        (filtered["population"] >= pop_range[0])
        & (filtered["population"] <= pop_range[1])
    )
)
filtered = filtered[pop_mask]

st.sidebar.write(f"Showing **{len(filtered)}** counties")



# ---- Categorical map ----
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
    "Best-Aligned Care": "#1B5E20",        # dark green
    "Low Gap": "#81C784",                  # light green
    "Moderate Gap": "#FFEB3B",             # yellow
    "High Gap": "#FF8A65",                 # light red
    "Critical Gap": "#B71C1C",             # dark red
    "No / Insufficient Data": "#BDBDBD",   # gray
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
        "SCAI": True,
        "access_rank": True,
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
    "SCAI",
    "access_rank",
    "display_category",
    "stroke_mortality_rate",
    "uninsured_rate",
    "hospitals_reporting",
    "care_gap_index",
    "data_status",
]
cols_to_show = [c for c in cols_to_show if c in filtered.columns]

st.dataframe(
    filtered[cols_to_show].sort_values("access_rank", ascending=True),
    use_container_width=True,
    hide_index=True,
)
