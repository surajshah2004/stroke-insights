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
    "Best-Aligned Care",
    "Low Gap",
    "Moderate Gap",
    "High Gap",
    "Critical Gap",
    "No / Insufficient Data",
]

COLOR_MAP = {
    "Best-Aligned Care": "#1B5E20",
    "Low Gap": "#81C784",
    "Moderate Gap": "#FFEB3B",
    "High Gap": "#FF8A65",
    "Critical Gap": "#B71C1C",
    "No / Insufficient Data": "#BDBDBD",
}

DISPLAY_NAMES = {
    "stroke_mortality_rate": "Stroke mortality (deaths per 100,000)",
    "uninsured_rate": "Uninsured rate (%)",
    "hospitals_reporting": "Hospitals reporting",
    "care_gap_index": "Care gap index",
    "burden_index": "Burden index",
    "supply_score": "Supply score",
    "SCAI": "SCAI",
    "access_rank": "Access rank (1 = best)",
}


# -----------------------------
# Load + preprocess
# -----------------------------
@st.cache_data
def load_county_data() -> pd.DataFrame:
    df = pd.read_csv(CLEAN / "stroke_care_gap_index.csv", dtype={"county_fips": str})

    # Ensure FIPS are 5-digit strings
    if "county_fips" in df.columns:
        df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

    # Make key fields numeric if present
    numeric_cols = [
        "care_gap_index",
        "population",
        "hospitals_reporting",
        "stroke_mortality_rate",
        "uninsured_rate",
        "burden_index",
        "supply_score",
        "SCAI",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Default data_status
    if "data_status" not in df.columns:
        df["data_status"] = "VALID"

    # Normalize uninsured_rate to percent if it looks like a fraction
    if "uninsured_rate" in df.columns:
        med_unins = df["uninsured_rate"].median(skipna=True)
        if pd.notna(med_unins) and med_unins <= 1.0:
            df["uninsured_rate"] = df["uninsured_rate"] * 100.0

    # Compute Burden Index if missing and ingredients exist
    # burden_index = log1p( stroke_mortality_rate * (uninsured_rate/100) )
    if "burden_index" not in df.columns:
        if {"stroke_mortality_rate", "uninsured_rate"} <= set(df.columns):
            prod = df["stroke_mortality_rate"] * (df["uninsured_rate"] / 100.0)
            df["burden_index"] = np.log1p(prod)

    # Compute SCAI if missing: 1 - normalized care_gap_index (higher = better access)
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

    # access_rank: 1 = best access (based on SCAI)
    df["access_rank"] = df["SCAI"].rank(method="dense", ascending=False)

    # Base SCAI category column (used for filtering + tables)
    df["display_category"] = "No / Insufficient Data"

    valid_mask = (
        (df["data_status"] == "VALID")
        & df.get("care_gap_index", pd.Series(index=df.index, dtype=float)).notna()
        & df.get("SCAI", pd.Series(index=df.index, dtype=float)).notna()
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


def make_quintile_category(
    df: pd.DataFrame,
    metric_col: str,
    higher_is_better: bool,
    data_status_col: str = "data_status",
) -> pd.Series:
    """
    Returns a categorical Series with CATEGORY_ORDER labels based on quintiles of `metric_col`.
    - Applies VALID + metric-not-null rule; everything else => "No / Insufficient Data"
    - Uses the same 5-category scheme as your SCAI display_category.
    - Inverts labels when higher_is_better=False (e.g., burden where higher = worse).
    """
    out = pd.Series("No / Insufficient Data", index=df.index, dtype="object")

    if metric_col not in df.columns:
        return out

    valid_mask = (df.get(data_status_col, "VALID") == "VALID") & df[metric_col].notna()
    valid = df.loc[valid_mask, metric_col].copy()

    if valid.empty:
        return out

    # Quintile labels (low -> high). We assign then invert if needed.
    quintile_labels_low_to_high = [
        "Critical Gap",
        "High Gap",
        "Moderate Gap",
        "Low Gap",
        "Best-Aligned Care",
    ]

    # Compute quintiles (rank-based bins)
    try:
        binned = pd.qcut(
            valid,
            q=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
            labels=quintile_labels_low_to_high,
            duplicates="drop",
        ).astype(str)
    except ValueError:
        # If not enough unique values for qcut, keep insufficient
        return out

    # If higher is better, we want high values => Best-Aligned.
    # Our current labels assign low values => Critical, high => Best, which matches higher_is_better=True.
    # If higher is worse (burden), invert mapping so low burden => Best, high burden => Critical.
    if not higher_is_better:
        invert_map = {
            "Critical Gap": "Best-Aligned Care",
            "High Gap": "Low Gap",
            "Moderate Gap": "Moderate Gap",
            "Low Gap": "High Gap",
            "Best-Aligned Care": "Critical Gap",
        }
        binned = binned.map(invert_map)

    out.loc[valid_mask] = binned
    return out


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Stroke Care Access Explorer", layout="wide")
st.title("U.S. Stroke Care Access Explorer")

st.caption(
    "County-level Stroke Care Access Index (SCAI) combining stroke mortality (deaths per 100,000), "
    "uninsured rate (percent), hospital supply, and performance."
)

# -----------------------------
# Load data
# -----------------------------
df = load_county_data()


# -----------------------------
# Sidebar controls + filters
# -----------------------------
st.sidebar.header("Filters")

# Map toggle (show only options that exist)
metric_options = [("SCAI", "SCAI")]
if "supply_score" in df.columns:
    metric_options.append(("Supply score", "supply_score"))
if "burden_index" in df.columns:
    metric_options.append(("Burden index", "burden_index"))

metric_label_to_col = dict(metric_options)

map_metric_label = st.sidebar.radio(
    "Map metric",
    options=[x[0] for x in metric_options],
    index=0,
    help="Switch the choropleth to show SCAI, supply score, or burden index (quintiles with the same color scheme).",
)
map_metric_col = metric_label_to_col[map_metric_label]

# State filter
states = sorted(df["state"].dropna().unique()) if "state" in df.columns else []
state_filter = st.sidebar.multiselect(
    "State(s)",
    options=states,
    default=None,
    help="Limit the map and table to selected states.",
)

# Category filter (this filter remains based on SCAI categories to keep behavior consistent)
available_categories = (
    sorted(df["display_category"].dropna().unique().tolist())
    if "display_category" in df.columns
    else []
)
category_filter = st.sidebar.multiselect(
    "Category (SCAI-based)",
    options=available_categories,
    default=available_categories,
    help="Filters remain based on the original SCAI-based category, even if you toggle the map metric.",
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
# Population summary by category (SCAI categories)
# -----------------------------
st.subheader("Population by care gap category")

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

    # Format columns
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
            "display_category": "Category",
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
        "Categories are ordered from best to worst stroke care access (SCAI quintiles). "
        "Total population reflects the sum across counties in each category; "
        "mean, SD, and min/max describe the distribution of county populations within each category "
        "in the current filtered view."
    )
else:
    st.info("Population summary is unavailable because population is missing in the current view.")


# -----------------------------
# Map categories (toggle-controlled)
# -----------------------------
# Determine direction
# - SCAI: higher better
# - Supply score: assume higher better
# - Burden index: higher worse (invert)
higher_is_better = True
if map_metric_col == "burden_index":
    higher_is_better = False

filtered = filtered.copy()
filtered["map_category"] = make_quintile_category(
    filtered,
    metric_col=map_metric_col,
    higher_is_better=higher_is_better,
)

# Enforce ordered categories for consistent legend ordering
filtered["map_category"] = pd.Categorical(
    filtered["map_category"],
    categories=CATEGORY_ORDER,
    ordered=True,
)


# -----------------------------
# Choropleth map
# -----------------------------
st.subheader(f"Map: {map_metric_label} (quintiles)")

geojson_url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

hover_data = {
    "burden_index": ":.3f",
    "supply_score": ":.3f",
    "SCAI": ":.3f",
    "access_rank": ":.0f",
    "stroke_mortality_rate": ":.1f",
    "uninsured_rate": ":.1f",
    "population": ":,",
    "hospitals_reporting": True,
    "care_gap_index": ":.3f",
    "display_category": True,
    "map_category": True,
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
    labels={"map_category": "Category", "display_category": "SCAI category", **DISPLAY_NAMES},
)

fig.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0})
st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# County table (still shows SCAI category; map category is also shown)
# -----------------------------
st.subheader("County-level details")

cols_to_show = [
    "state",
    "county_name",
    "population",
    "SCAI",
    "access_rank",
    "display_category",
    "map_category",
    "stroke_mortality_rate",
    "uninsured_rate",
    "burden_index",
    "supply_score",
    "hospitals_reporting",
    "care_gap_index",
    "data_status",
]
cols_to_show = [c for c in cols_to_show if c in filtered.columns]

table_df = filtered[cols_to_show].copy()

rename_for_display = {
    "stroke_mortality_rate": "Stroke mortality (deaths per 100,000)",
    "uninsured_rate": "Uninsured rate (%)",
    "access_rank": "Access rank (1 = best)",
    "display_category": "SCAI category",
    "map_category": "Map category",
    "burden_index": "Burden index",
    "supply_score": "Supply score",
}
table_df = table_df.rename(columns=rename_for_display)

sort_col = "Access rank (1 = best)" if "Access rank (1 = best)" in table_df.columns else None
if sort_col:
    table_df = table_df.sort_values(sort_col, ascending=True)

st.dataframe(table_df, use_container_width=True, hide_index=True)
