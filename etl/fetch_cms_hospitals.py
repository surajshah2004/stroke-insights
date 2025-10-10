# etl/fetch_cms_hospitals.py
# Robust CMS "Hospital General Information" (xubh-q36u)
# Writes:
#   data_raw/cms_hospital_info.csv
#   data_clean/cms_hospital_info.csv

import os
import pathlib
import traceback
from io import StringIO

import pandas as pd
import requests

DOMAIN = "data.cms.gov"
DATASET = "xubh-q36u"
EXPORT = f"https://{DOMAIN}/api/views/{DATASET}/rows.csv?accessType=DOWNLOAD"
JSON_API = f"https://{DOMAIN}/resource/{DATASET}.json"
CHUNK = 50000

OUT_RAW = pathlib.Path("data_raw/cms_hospital_info.csv")
OUT_CLEAN = pathlib.Path("data_clean/cms_hospital_info.csv")


def ensure_dirs():
    OUT_RAW.parent.mkdir(exist_ok=True)
    OUT_CLEAN.parent.mkdir(exist_ok=True)


def placeholder(msg: str):
    print("WARN:", msg)
    ensure_dirs()
    pd.DataFrame().to_csv(OUT_RAW, index=False)
    pd.DataFrame().to_csv(OUT_CLEAN, index=False)


def fetch_export_csv() -> pd.DataFrame:
    print("INFO: fetching export CSV …")
    r = requests.get(EXPORT, timeout=180)
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text), dtype=str, low_memory=False)


def fetch_http_json_paged() -> pd.DataFrame:
    print("INFO: fetching HTTP JSON (paged) …")
    headers = {"X-App-Token": os.getenv("SOCRATA_APP_TOKEN", "")}
    rows, offset = [], 0
    while True:
        r = requests.get(
            JSON_API,
            params={"$limit": CHUNK, "$offset": offset},
            headers=headers,
            timeout=120,
        )
        if r.status_code != 200:
            print("INFO: HTTP JSON status", r.status_code, r.text[:200])
            break
        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        offset += CHUNK
        if len(chunk) < CHUNK:
            break
    return pd.DataFrame.from_records(rows)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure CCN column exists
    if "ccn" not in df.columns:
        for c in ["provider_id", "provider_number"]:
            if c in df.columns:
                df = df.rename(columns={c: "ccn"})
                break
    # Ensure hospital_name exists
    if "hospital_name" not in df.columns:
        for alt in ["facility_name", "hospital name", "facility name", "facility_name_1"]:
            if alt in df.columns:
                df["hospital_name"] = df[alt]
                break
    return df


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

    df_raw = df.copy()
    df_clean = normalize_columns(df.copy())

    print(f"OK: hospital rows = {len(df_clean):,}")
    df_raw.to_csv(OUT_RAW, index=False)
    df_clean.to_csv(OUT_CLEAN, index=False)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        placeholder("Unhandled exception")
