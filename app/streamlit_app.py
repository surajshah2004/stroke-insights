from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------------
# Paths
# -----------------------------
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data_clean"


# -----------------------------
# Constants
# -----------------------------
CATEGORY_ORDER = [
    "90–100th percentile",
    "60–90th percentile",
    "30–60th percentile",
    "10–30th percentile",
    "0–10th percentile",
    "No / Insufficient Data",
]

COLOR_MAP = {
    "90–100th percentile": "#1B5E20",
    "60–90th percentile": "#81C784",
    "30–60th percentile": "#FFEB3B",
    "10–30th percentile": "#FF8A65",
    "0–10th percentile": "#B71C1C",
    "No / Insufficient Data": "#BDBDBD",
}

DISPLAY_NAMES = {
    "stroke_mortality_rate": "Stroke mortality (deaths per 100,000)",
    "uninsured_rate": "Uninsured rate (%)",
    "hospitals_reporting": "Hospitals reporting",
    "SCAI": "Stroke Care Alignment Index (SCAI)",
    "access_rank": "SCAI rank (1 = highest)",
}


# -----------------------------
# Publication link (set after acceptance)
# -----------------------------
PAPER_URL = None  # e.g., "https://doi.org/10.xxxx/xxxx" or the final journal landing page


# -----------------------------
# Load + preprocess
# -----------------------------
@st.cache_data
def load_county_data() -> pd.DataFrame:
    df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

    # Ensure FIPS are 5-digit strings
    if "county_fips" in df.columns:
        df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

    # Default data_status then normalize
    if "data_status" not in df.columns:
        df["data_status"] = "VALID"

    df["data_status"] = (
        df["data_status"]
        .fillna("VALID")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Helper to coerce numeric columns more robustly
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

    # Coerce relevant numeric columns
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

    # Normalize uninsured_rate to percent if it looks like a fraction
    if "uninsured_rate" in df.columns:
        med_unins = df["uninsured_rate"].median(skipna=True)
        if pd.notna(med_unins) and med_unins <= 1.0:
            df["uninsured_rate"] = df["uninsured_rate"] * 100.0

    # Preserve the original underlying calculation if this input is absent
    # burden_index = log1p( stroke_mortality_rate * (uninsured_rate/100) )
    if "burden_index" not in df.columns:
        if {"stroke_mortality_rate", "uninsured_rate"} <= set(df.columns):
            prod = df["stroke_mortality_rate"] * (df["uninsured_rate"] / 100.0)
            df["burden_index"] = np.log1p(prod)

    # Compute SCAI if missing: 1 - normalized care_gap_index (higher = more favorable)
    if "SCAI" not in df.columns:
        if "care_gap_index" in df.columns:
            care = pd.to_numeric(df["care_gap_index"], errors="coerce")
            min_val = care.min()
            max_val = care.max()
            if pd.notna(min_val) and pd.notna(max_val) and max_val > min_val:
                care_norm = (care - min_val) / (max_val - min_val)
                df["SCAI"] = 1.0 - care_norm
            else:
                df["SCAI"] = np.nan
        else:
            df["SCAI"] = np.nan

    # access_rank: 1 = highest SCAI
    df["access_rank"] = df["SCAI"].rank(method="dense", ascending=False)

    # Base national SCAI percentile group (used for filtering + tables)
    df["display_category"] = "No / Insufficient Data"

    # Treat anything containing "INSUFFICIENT" as not valid; everything else counts as valid
    is_valid = ~df["data_status"].str.contains("INSUFFICIENT", na=False)

    valid_mask = (
        is_valid
        & df.get("care_gap_index", pd.Series(index=df.index, dtype=float)).notna()
        & df.get("SCAI", pd.Series(index=df.index, dtype=float)).notna()
    )

    valid = df[valid_mask].copy()
    if len(valid) > 0:
        # Percentiles are assigned nationally, before sidebar filtering.
        pct = valid["SCAI"].rank(method="average", pct=True)
        valid["display_category"] = pd.cut(
            pct,
            bins=[0, 0.1, 0.3, 0.6, 0.9, 1.0],
            labels=[
                "0–10th percentile",
                "10–30th percentile",
                "30–60th percentile",
                "60–90th percentile",
                "90–100th percentile",
            ],
            include_lowest=True,
        ).astype(str)

        df.loc[valid.index, "display_category"] = valid["display_category"]

    return df


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Stroke Care Alignment Explorer", layout="wide")
st.title("U.S. Stroke Care Alignment Explorer")

st.caption(
    "Exploratory Stroke Care Alignment Index (SCAI) combining county-level stroke mortality, "
    "uninsured rates, CMS hospital performance, and number of reporting hospitals. "
    "Higher SCAI values indicate a more favorable relationship among these measures; "
    "SCAI does not directly measure access to care."
)

# -----------------------------
# PI-requested links (top of page)
# -----------------------------
st.markdown("**Clinical resources (UCSF):**")
st.markdown(
    "- [UCSF Cerebrovascular Neurosurgery Clinic](https://www.ucsfhealth.org/clinics/cerebrovascular-neurosurgery-clinic)"
)
st.markdown(
    "- [UCSF Comprehensive Stroke Center](https://www.ucsfhealth.org/list-of-clinics/comprehensive-stroke-center)"
)

st.markdown("**Publication:**")
if PAPER_URL:
    st.markdown(f"- [Study publication link]({PAPER_URL})")
else:
    st.markdown("- _Link will be added upon acceptance._")


# -----------------------------
# Load data
# -----------------------------
df = load_county_data()


# -----------------------------
# Sidebar controls + filters
# -----------------------------
st.sidebar.header("Filters")


# State filter
states = sorted(df["state"].dropna().unique()) if "state" in df.columns else []
state_filter = st.sidebar.multiselect(
    "State(s)",
    options=states,
    default=None,
    help="Limit the map and table to selected states.",
)

# Category filter (SCAI-based)
available_categories = (
    sorted(df["display_category"].dropna().unique().tolist())
    if "display_category" in df.columns
    else []
)
category_filter = st.sidebar.multiselect(
    "SCAI percentile group",
    options=available_categories,
    default=available_categories,
    help="Filter counties by their national SCAI percentile group.",
)

# Population slider
if "population" in df.columns and df["population"].notna().any():
    pop_max = int(df["population"].max(skipna=True))
else:
    pop_max = 0

pop_range = st.sidebar.slider(
    "Population range",
    min_value=0,
    max_value=pop_max,
    value=(0, pop_max),
    step=1000 if pop_max >= 1000 else 1,
)

# Apply filters
filtered = df.copy()

if state_filter and "state" in filtered.columns:
    filtered = filtered[filtered["state"].isin(state_filter)]

if category_filter and "display_category" in filtered.columns:
    filtered = filtered[filtered["display_category"].isin(category_filter)]

if "population" in filtered.columns:
    pop_mask = filtered["population"].isna() | (
        (filtered["population"] >= pop_range[0]) & (filtered["population"] <= pop_range[1])
    )
    filtered = filtered[pop_mask]

st.sidebar.write(f"Showing **{len(filtered)}** counties")


# -----------------------------
# Population summary by SCAI percentile group
# -----------------------------
st.subheader("Population by SCAI percentile group")

if {"display_category", "population"} <= set(filtered.columns) and filtered["population"].notna().any():
    pop_summary = (
        filtered.dropna(subset=["population"])
        .groupby("display_category", observed=True)
        .agg(
            total_population=("population", "sum"),
            mean_county_population=("population", "mean"),
            std_county_population=("population", "std"),
            min_county_population=("population", "min"),
            max_county_population=("population", "max"),
        )
        .reset_index()
    )

    total_pop = pop_summary["total_population"].sum()
    pop_summary["share_of_population_percent"] = (
        (pop_summary["total_population"] / total_pop) * 100.0 if total_pop else 0.0
    )

    pop_summary["display_category"] = pd.Categorical(
        pop_summary["display_category"],
        categories=CATEGORY_ORDER,
        ordered=True,
    )
    pop_summary = pop_summary.sort_values("display_category")

    int_cols = [
        "total_population",
        "mean_county_population",
        "std_county_population",
        "min_county_population",
        "max_county_population",
    ]
    for c in int_cols:
        if c in pop_summary.columns:
            pop_summary[c] = pop_summary[c].fillna(0).round(0).astype(int)

    pop_summary["share_of_population_percent"] = pop_summary["share_of_population_percent"].round(2)

    pop_summary_display = pop_summary.rename(
        columns={
            "display_category": "SCAI percentile group",
            "total_population": "Total population",
            "share_of_population_percent": "Share of population (%)",
            "mean_county_population": "Mean county population",
            "std_county_population": "SD county population",
            "min_county_population": "Min county population",
            "max_county_population": "Max county population",
        }
    )

    st.dataframe(pop_summary_display, use_container_width=True, hide_index=True)
    st.caption(
        "Groups are ordered from highest to lowest SCAI percentiles (10/20/30/30/10 grouping). "
        "Total population reflects the sum across counties in each category; "
        "mean, SD, and min/max describe the distribution of county populations within each category "
        "in the current filtered view."
    )
else:
    st.info("Population summary is unavailable because population is missing in the current view.")


# -----------------------------
# Map categories (fixed national SCAI percentile groups)
# -----------------------------
filtered = filtered.copy()
filtered["map_category"] = pd.Categorical(
    filtered["display_category"].astype(str).str.strip(),
    categories=CATEGORY_ORDER,
    ordered=True,
)


# -----------------------------
# Debug panel
# -----------------------------
with st.expander("Debug: map metric coverage"):
    st.write("Metric: SCAI")
    st.write("Non-null metric values:", int(filtered["SCAI"].notna().sum()))
    st.write("Unique metric values:", int(filtered["SCAI"].nunique(dropna=True)))
    st.write("Metric min / max:", filtered["SCAI"].min(), filtered["SCAI"].max())

    if "data_status" in filtered.columns:
        st.write("Top data_status values:")
        st.write(filtered["data_status"].value_counts(dropna=False).head(10))

    if "map_category" in filtered.columns:
        st.write("Map category counts:")
        st.write(filtered["map_category"].value_counts(dropna=False))


# -----------------------------
# Choropleth map
# -----------------------------
st.subheader("Map: SCAI (national percentile groups)")

geojson_url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

hover_data = {
    "SCAI": ":.3f",
    "access_rank": ":.0f",
    "stroke_mortality_rate": ":.1f",
    "uninsured_rate": ":.1f",
    "population": ":,",
    "hospitals_reporting": True,
    "display_category": True,
    "data_status": True,
    "state": True,
    "county_fips": False,
}
hover_data = {k: v for k, v in hover_data.items() if k in filtered.columns}

fig = px.choropleth(
    filtered,
    geojson=geojson_url,
    locations="county_fips" if "county_fips" in filtered.columns else None,
    color="map_category" if "map_category" in filtered.columns else None,
    color_discrete_map=COLOR_MAP,
    scope="usa",
    hover_name="county_name" if "county_name" in filtered.columns else None,
    hover_data=hover_data,
    labels={"map_category": "SCAI percentile group", "display_category": "SCAI percentile group", **DISPLAY_NAMES},
)

fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0})
st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# County table
# -----------------------------
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
    "data_status",
]
cols_to_show = [c for c in cols_to_show if c in filtered.columns]

table_df = filtered[cols_to_show].copy()

rename_for_display = {
    "stroke_mortality_rate": "Stroke mortality (deaths per 100,000)",
    "uninsured_rate": "Uninsured rate (%)",
    "access_rank": "SCAI rank (1 = highest)",
    "display_category": "SCAI percentile group",
}
table_df = table_df.rename(columns=rename_for_display)

sort_col = "SCAI rank (1 = highest)" if "SCAI rank (1 = highest)" in table_df.columns else None
if sort_col:
    table_df = table_df.sort_values(sort_col, ascending=True)

st.dataframe(table_df, use_container_width=True, hide_index=True)
