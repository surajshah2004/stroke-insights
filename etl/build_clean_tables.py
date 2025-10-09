# etl/build_clean_tables.py
# Writes analysis-ready tables and tolerates empty/missing inputs by creating header-only CSVs.

import json
import pathlib
import pandas as pd
from pandas.errors import EmptyDataError

DATA = pathlib.Path("data_clean")
DATA.mkdir(exist_ok=True)

def read_safe(filename: str):
    p = DATA / filename
    if not p.exists() or p.stat().st_size == 0:
        print(f"INFO: {filename} missing/empty; treating as None")
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

def write_header_only(path: pathlib.Path, columns):
    df = pd.DataFrame(columns=columns)
    df.to_csv(path, index=False)
    print(f"INFO: wrote header-only {path.name} with columns {columns}")

def build_hospital_profile():
    out = DATA / "hospital_profile.csv"
    o = read_safe("cms_stroke_outcomes.csv")
    h = read_safe("cms_hospital_info.csv")

    # default header set
    default_cols = ["ccn","hospital_name","address","city","state","zip_code",
                    "county_name","phone_number","lat","lon","mortality_30d","readmit_30d"]

    if o is None or h is None:
        print("INFO: Skipping hospital join (missing outcomes or hospital info).")
        return write_header_only(out, default_cols)

    # Normalize CCN
    if "ccn" not in o.columns and "provider_id" in o.columns:
        o = o.rename(columns={"provider_id": "ccn"})
    if "ccn" not in h.columns and "provider_id" in h.columns:
        h = h.rename(columns={"provider_id": "ccn"})

    # If outcomes schema lacks measure_id, just output hospital info headers
    if "measure_id" not in o.columns:
        print("WARN: outcomes missing 'measure_id'; writing hospital info headers only.")
        return write_header_only(out, default_cols)

    keep_ids = {"MORT_30_STK": "mortality_30d", "READM_30_STK": "readmit_30d"}
    o = o[o["measure_id"].isin(keep_ids)].copy()

    if o.empty:
        print("INFO: No stroke outcome rows. Writing header-only hospital_profile.")
        return write_header_only(out, default_cols)

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

    h2 = h.copy()
    for c in ["hospital_name","address","city","state","zip_code","county_name","phone_number"]:
        if c not in h2.columns:
            h2[c] = pd.NA

    # lat/lon from 'location'
    h2["lat"], h2["lon"] = pd.NA, pd.NA
    if "location" in h2.columns:
        def latlon(v):
            try:
                v = json.loads(v) if isinstance(v, str) else v
                if isinstance(v, dict):
                    if "latitude" in v and "longitude" in v:
                        return float(v["latitude"]), float(v["longitude"])
                    if "coordinates" in v and isinstance(v["coordinates"], (list, tuple)) and len(v["coordinates"]) == 2:
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

    # Ensure all default columns exist (order them nicely)
    for c in default_cols:
        if c not in prof.columns:
            prof[c] = pd.NA
    prof = prof[default_cols]

    prof.to_csv(out, index=False)
    print(f"OK: wrote {len(prof):,} rows to {out}")

def build_county_profile():
    out = DATA / "county_profile.csv"
    cdc = read_safe("cdc_stroke_mortality_county.csv")
    acs = read_safe("acs_uninsured_county.csv")

    default_cols = ["fips","state","county","state_name","county_name","death_rate","pct_uninsured"]

    if cdc is None:
        print("INFO: Skipping county join (missing CDC file).")
        return write_header_only(out, default_cols)

    df = cdc.copy()

    # Build FIPS
    fips = None
    if "fips" in df.columns:
        fips = df["fips"].astype(str).str.zfill(5)
    elif all(c in df.columns for c in ["state_fips", "county_fips"]):
        fips = df["state_fips"].astype(str).str.zfill(2) + df["county_fips"].astype(str).str.zfill(3)
    elif all(c in df.columns for c in ["state", "county"]):
        fips = df["state"].astype(str).str.zfill(2) + df["county"].astype(str).str.zfill(3)

    if fips is None:
        print("WARN: Could not construct FIPS; writing header-only county_profile.")
        return write_header_only(out, default_cols)

    df["fips"] = fips

    if acs is not None and "fips" in acs.columns:
        df = df.merge(acs[["fips", "pct_uninsured"]], on="fips", how="left")
    else:
        df["pct_uninsured"] = pd.NA

    # Try to keep common columns if present
    cols = [c for c in default_cols if c in df.columns]
    if "fips" not in cols:
        cols = ["fips"] + cols
    df = df[cols]

    df.to_csv(out, index=False)
    print(f"OK: wrote {len(df):,} rows to {out}")

if __name__ == "__main__":
    build_hospital_profile()
    build_county_profile()
