# etl/fetch_cms_hospitals.py
# Robust CMS "Hospital General Information" (xubh-q36u)
# Writes:
#   data_raw/cms_hospital_info.csv
#   data_clean/cms_hospital_info.csv

import os, pathlib, traceback
import pandas as pd
import requests

DOMAIN   = "data.cms.gov"
DATASET  = "xubh-q36u"
EXPORT   = f"https://{DOMAIN}/api/views/{DATASET}/rows.csv?accessType=DOWNLOAD"
JSON_API = f"https://{DOMAIN}/resource/{DATASET}.json"

CHUNK = 50000
OUT_RAW   = pathlib.Path("data_raw/cms_hospital_info.csv")
OUT_CLEAN = pathlib.Path("data_clean/cms_hospital_info.csv")

def ensure_dirs():
    OUT_RAW.parent.mkdir(exist_ok=True); OUT_CLEAN.parent.mkdir(exist_ok=True)

def placeholder(msg):
    print("WARN:", msg); ensure_dirs()
    pd.DataFrame().to_csv(OUT_RAW, index=False)
    pd.DataFrame().to_csv(OUT_CLEAN, index=False)

def fetch_export_csv():
    # Export CSV = full dataset, no token required
    print("INFO: fetching export CSV …")
    return pd.read_csv(EXPORT, dtype=str, low_memory=False)

def fetch_http_json_paged():
    print("INFO: fetching HTTP JSON (paged) …")
    headers = {"X-App-Token": os.getenv("SOCRATA_APP_TOKEN","")}
    rows, offset = [], 0
    while True:
        r = requests.get(JSON_API, params={"$limit": CHUNK, "$offset": offset},
                         headers=headers, timeout=120)
        if r.status_code != 200:
            print("INFO: HTTP JSON status", r.status_code, r.text[:200]); break
        chunk = r.json()
        if not chunk: break
        rows.extend(chunk); offset += CHUNK
        if len(chunk) < CHUNK: break
    return pd.DataFrame.from_records(rows)

def main():
    print(f"Fetching CMS hospital info ({DATASET}) …")
    ensure_dirs()

    df = pd.DataFrame()
    try:
        df = fetch_export_csv()
    except Exception as e:
        print("INFO: export failed:", e)
    if df is None or df.empty:
        try:
            df = fetch_http_json_paged()
        except Exception as e:
            print("INFO: HTTP JSON fallback failed:", e)

    if df is None or df.empty:
        return placeholder("CMS hospital info empty")

    # Raw snapshot
    df_raw = df.copy()

    # Light clean
    df_clean = df.copy()
    # Standardize CCN
    for c in ["ccn", "provider_id", "provider_number"]:
        if c in df_clean.columns:
            if c != "ccn":
