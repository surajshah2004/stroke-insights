# etl/build_clean_tables.py
# Robust joins that tolerate empty/placeholder CSVs from earlier ETL steps.

import json
import pathlib
import pandas as pd
from pandas.errors import EmptyDataError

DATA = pathlib.Path("data_clean")
DATA.mkdir(exist_ok=True)

def read_safe(filename: str):
    """Return a DataFrame or None if file missing/empty/unreadable."""
    p = DATA / filename
    if not p.exists() or p.stat().st_size == 0:
        print(f"INFO: {filename} missing or empty; treating as None")
        return None
    try:
        return pd.read_csv(p, dtype=str, low_memory=False)
    except EmptyDataError:
        print(f"INFO: {filename} is empty CSV; treating as None")
        return None
    except Exception as e:
        print(f"WARN: failed to read {filename}: {e}; treating as None")
        return None

def num(x):
    return pd.to_numeric(x, errors="coerce")

def build_hospital_profile():
    out = DATA / "hospital_profile.csv"
    o = read_safe("cms_stroke_outcomes.csv")
    h = read_safe("cms_hospital_info.csv")

    if o is None or h is None:
        print("INFO: Skipping hospital join (missing outcomes or hospital info). Writing empty output.")
        pd.DataFrame().to_csv(out, index=False)
        return

    # Normalize CCN column names if needed
    if "ccn" not in o.columns and "provider_id" in o.columns:
        o = o.rename(columns={"provider_id": "ccn"})
    if "ccn" not in h.columns and "provider_id" in h.columns:
        h = h.rename(columns={"provider_id": "ccn"})

    # Keep just ischemic stroke measures (mortality/readmission)
    if "measure_id" in o.columns:
        keep_ids = {"MORT_30_STK": "mortality_30d", "READM_30_STK": "readmit_30d"}
        o = o[o["measure_id"].isin(keep_ids)].copy()
        if "end_date" in o.columns:
            o["end_date_dt"] = pd.to_datetime(o["end_date"], errors="coerce")
            o = (
                o.sort_values(["ccn", "measure_id", "end_date_dt"])
                 .drop_duplicates(["ccn", "measure_id"], keep="last")
            )
        o["score_num"] = num(o.get("score"))
        wide = (
            o.pivot_table(index="ccn", columns="measure_id", values="score_num", aggfunc="first")
             .rename(columns=keep_ids)
             .reset_index()
        )
    else:
        # If schema changed and measure_id isn't present, pass through basic hospital info
        print("WARN: outcomes missing 'measure_id'; writing hospital info without outcomes.")
        h.to_csv(out, index=False)
        return

    # Ensure expected hospital columns exist
    h2 = h.copy()
    for c in ["hospital_name", "address", "city", "state", "zip_code", "county_name", "phone_number"]:
        if c not in h2.columns:
            h2[c] = pd.NA

    # Extract lat/lon from 'location' if present
    h2["lat"], h2["lon"] = pd.NA, pd.NA
    if "location" in h2.columns:
        def latlon(v):
            try:
                v = json.loads(v) if isinstance(v, str) else v
                if isinstance(v, dict):
                    if "latitude" in v and "longitude" in v:
                        return float(v["latitude"]), float(v["longitude"])
                    if "coordinates" in v and isinstance(v["coordinates"], (list, tuple)) and len(v["coordinates"]) == 2:
                        # Socrata sometimes stores [lon, lat]
                        return float(v["coordinates"][1]), float(v["coordinates"][0])
            except Exception:
                pass
            return (pd.NA, pd.NA)
        pairs = h2["location"].apply(latlon)
        h2["lat"] = pairs.apply(lambda t: t[0])
        h2["lon"] = pairs.apply(lambda t: t[1])

    prof = h2.merge(wide, on="ccn", how="left")
    for c in ["mortality_30d", "readmit_30d", "lat", "lon"]:
        if c in prof.columns:
            prof[c] = num(prof[c])

    prof.to_csv(out, index=False)
    print(f"OK: wrote {len(prof):,} rows to data_clean/hospital_profile.csv")

def build_county_profile():
    out = DATA / "county_profile.csv"
    cdc = read_safe("cdc_stroke_mortality_county.csv")
    acs = read_safe("acs_uninsured_county.csv")

    if cdc is None:
        print("INFO: Skipping county join (missing CDC file). Writing empty output.")
        pd.DataFrame().to_csv(out, index=False)
        return

    df = cdc.copy()

    # Build FIPS from available columns
    fips = None
    if "fips" in df.columns:
        fips = df["fips"].astype(str).str.zfill(5)
    elif all(c in df.columns for c in ["state_fips", "county_fips"]):
        fips = df["state_fips"].astype(str).str.zfill(2) + df["county_fips"].astype(str).str.zfill(3)
    elif all(c in df.columns for c in ["state", "county"]):
        fips = df["state"].astype(str).str.zfill(2) + df["county"].astype(str).str.zfill(3)

    if fips is None:
        print("WARN: Could not construct FIPS; saving CDC pass-through.")
        df.to_csv(out, index=False)
        return

    df["fips"] = fips
    if acs is not None and "fips" in acs.columns:
        df = df.merge(acs[["fips", "pct_uninsured"]], on="fips", how="left")
    else:
        df["pct_uninsured"] = pd.NA

    df.to_csv(out, index=False)
    print(f"OK: wrote {len(df):,} rows to data_clean/county_profile.csv")

if __name__ == "__main__":
    build_hospital_profile()
    build_county_profile()
