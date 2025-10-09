# etl/fetch_cms_outcomes.py
# Robust pull of CMS "Complications & Deaths – Hospital" with stroke measures
# Writes BOTH data_raw/cms_complications_deaths_stroke.csv and
# data_clean/cms_stroke_outcomes.csv (placeholders if source is unavailable)

import os, json, traceback
import pathlib
import pandas as pd

OUT_RAW   = pathlib.Path("data_raw/cms_complications_deaths_stroke.csv")
OUT_CLEAN = pathlib.Path("data_clean/cms_stroke_outcomes.csv")

DOMAIN  = "data.cms.gov"
DATASET = "ynj2-r877"          # Complications & Deaths – Hospital (Socrata)
LIMIT   = 200000
WHERE   = "measure_id in('MORT_30_STK','READM_30_STK')"

def ensure_dirs():
    OUT_RAW.parent.mkdir(exist_ok=True)
    OUT_CLEAN.parent.mkdir(exist_ok=True)

def save(df_raw: pd.DataFrame, df_clean: pd.DataFrame):
    ensure_dirs()
    df_raw.to_csv(OUT_RAW, index=False)
    df_clean.to_csv(OUT_CLEAN, index=False)

def placeholder(msg: str):
    print("WARN:", msg)
    ensure_dirs()
    pd.DataFrame().to_csv(OUT_RAW, index=False)
    pd.DataFrame().to_csv(OUT_CLEAN, index=False)

def fetch_via_sodapy(where: str | None):
    try:
        from sodapy import Socrata
        client = Socrata(DOMAIN, os.getenv("SOCRATA_APP_TOKEN"), timeout=90)
        rows = client.get(DATASET, where=where, limit=LIMIT) if where else client.get(DATASET, limit=LIMIT)
        return pd.DataFrame.from_records(rows)
    except Exception as e:
        print("INFO: sodapy failed:", e)
        return None

def fetch_via_http(where: str | None):
    # Direct SoQL HTTP fallback
    import requests
    url = f"https://{DOMAIN}/resource/{DATASET}.json"
    params = {"$limit": str(LIMIT)}
    if where:
        params["$where"] = where
    r = requests.get(url, params=params, timeout=90)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def main():
    print(f"Fetching CMS outcomes from {DOMAIN}/{DATASET} …")

    # 1) Try stroke-only pull via sodapy
    df = fetch_via_sodapy(WHERE)
    if df is None or df.empty:
        print("INFO: stroke-only (sodapy) returned empty; trying HTTP fallback…")
        try:
            df = fetch_via_http(WHERE)
        except Exception as e:
            print("INFO: stroke-only (HTTP) failed:", e)

    # 2) If still empty, pull ALL rows then filter locally if possible
    if df is None or df.empty:
        print("INFO: fetching ALL rows to filter locally…")
        df_all = fetch_via_sodapy(None) or pd.DataFrame()
        if df_all.empty:
            try:
                df_all = fetch_via_http(None)
            except Exception as e:
                print("ERROR: full dataset fetch failed:")
                traceback.print_exc()
                return placeholder("Unable to fetch CMS dataset via both methods.")
        print(f"INFO: ALL rows fetched = {len(df_all):,}")
        cols = list(df_all.columns)
        print("INFO: Columns:", cols)

        if "measure_id" in df_all.columns:
            df = df_all[df_all["measure_id"].isin(["MORT_30_STK","READM_30_STK"])].copy()
        elif "measure_name" in df_all.columns:
            # very defensive: filter by name containing 'stroke'
            s = df_all["measure_name"].astype(str)
            df = df_all[s.str.contains("stroke", case=False, na=False)].copy()
        else:
            print("WARN: No measure_id/measure_name in dataset; saving full dataset as raw snapshot.")
            save(df_all, pd.DataFrame())
            return

    # 3) Save files (raw subset + light clean)
    if df is None or df.empty:
        print("WARN: CMS stroke subset is empty after all fallbacks.")
        return placeholder("CMS stroke subset empty")

    # Raw subset
    df_raw = df.copy()

    # Light normalization for clean file
    df_clean = df.copy()
    if "provider_id" in df_clean.columns and "ccn" not in df_clean.columns:
        df_clean = df_clean.rename(columns={"provider_id": "ccn"})

    # Helpful debug
    print(f"OK: stroke rows = {len(df_clean):,}")
    print("INFO: Clean columns:", list(df_clean.columns))

    save(df_raw, df_clean)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("FATAL: Uncaught exception in fetch_cms_outcomes.py")
        traceback.print_exc()
        # Never crash the workflow; write placeholders so later steps can proceed
        placeholder("Unhandled exception (see trace)")
