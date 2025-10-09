# etl/fetch_cms_hospitals.py
# Robust pull of CMS "Hospital General Information" (xubh-q36u)
# Writes BOTH:
#   data_raw/cms_hospital_info.csv
#   data_clean/cms_hospital_info.csv
# If the source is unavailable, writes empty placeholders (so the workflow keeps going).

import os, json, traceback, pathlib
import pandas as pd  # <-- keep this separate (important!)

DOMAIN  = "data.cms.gov"
DATASET = "xubh-q36u"          # Hospital General Information
CHUNK   = 50000                # page size

OUT_RAW   = pathlib.Path("data_raw/cms_hospital_info.csv")
OUT_CLEAN = pathlib.Path("data_clean/cms_hospital_info.csv")

def ensure_dirs():
    OUT_RAW.parent.mkdir(exist_ok=True)
    OUT_CLEAN.parent.mkdir(exist_ok=True)

def placeholder(msg: str):
    print("WARN:", msg)
    ensure_dirs()
    pd.DataFrame().to_csv(OUT_RAW, index=False)
    pd.DataFrame().to_csv(OUT_CLEAN, index=False)

def fetch_all_sodapy():
    try:
        from sodapy import Socrata
        client = Socrata(DOMAIN, os.getenv("SOCRATA_APP_TOKEN"), timeout=90)
        offset = 0
        rows_all = []
        while True:
            chunk = client.get(DATASET, limit=CHUNK, offset=offset)
            if not chunk:
                break
            rows_all.extend(chunk)
            offset += CHUNK
            if len(chunk) < CHUNK:
                break
        return pd.DataFrame.from_records(rows_all)
    except Exception as e:
        print("INFO: sodapy fetch failed:", e)
        return None

def fetch_all_http():
    import requests
    rows_all = []
    offset = 0
    while True:
        url = f"https://{DOMAIN}/resource/{DATASET}.json"
        params = {"$limit": str(CHUNK), "$offset": str(offset)}
        r = requests.get(url, params=params, timeout=90)
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        rows_all.extend(chunk)
        offset += CHUNK
        if len(chunk) < CHUNK:
            break
    return pd.DataFrame.from_records(rows_all)

def main():
    print(f"Fetching CMS hospital info from {DOMAIN}/{DATASET} …")

    ensure_dirs()
    df = fetch_all_sodapy()
    if df is None or df.empty:
        print("INFO: sodapy returned empty; trying HTTP fallback…")
        try:
            df = fetch_all_http()
        except Exception:
            print("ERROR: HTTP fallback failed:")
            traceback.print_exc()
            return placeholder("Unable to fetch CMS hospital info via both methods.")

    if df is None or df.empty:
        return placeholder("CMS hospital info returned no rows")

    # Raw snapshot
    df_raw = df.copy()

    # Light clean: standardize CCN column name
    df_clean = df.copy()
    if "provider_id" in df_clean.columns and "ccn" not in df_clean.columns:
        df_clean = df_clean.rename(columns={"provider_id":"ccn"})

    print(f"OK: hospital rows = {len(df_clean):,}")
    print("INFO: columns:", list(df_clean.columns))

    df_raw.to_csv(OUT_RAW, index=False)
    df_clean.to_csv(OUT_CLEAN, index=False)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("FATAL: Uncaught exception in fetch_cms_hospitals.py")
        traceback.print_exc()
        placeholder("Unhandled exception (see trace)")
