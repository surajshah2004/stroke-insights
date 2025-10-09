# etl/fetch_cms_outcomes.py
from sodapy import Socrata
import pandas as pd, pathlib

DOMAIN = "data.cms.gov"
DATASET = "ynj2-r877"  # Complications & Deaths – Hospital

OUT_RAW  = pathlib.Path("data_raw/cms_complications_deaths_stroke.csv")
OUT_CLEAN = pathlib.Path("data_clean/cms_stroke_outcomes.csv")

def main():
    OUT_RAW.parent.mkdir(exist_ok=True)
    OUT_CLEAN.parent.mkdir(exist_ok=True)
    client = Socrata(DOMAIN, None, timeout=60)

    # Try fetching by measure_id first (fast); fall back to name contains 'stroke'
    where = "measure_id in('MORT_30_STK','READM_30_STK')"
    rows = client.get(DATASET, where=where, limit=200000)
    df = pd.DataFrame.from_records(rows)

    if df.empty:
        rows = client.get(DATASET, limit=200000)
        tmp = pd.DataFrame.from_records(rows)
        df = tmp[tmp["measure_name"].str.contains("stroke", case=False, na=False)]

    if df.empty:
        raise SystemExit("No CMS stroke rows found; dataset/filters may have changed.")

    # Light normalization
    # Keep common fields; keep everything else too so we don't lose info across versions
    preferred = ["provider_id","hospital_name","address","city","state","zip_code",
                 "measure_id","measure_name","score","lower_estimate","higher_estimate",
                 "start_date","end_date","comparison_to_national","footnote"]
    for c in preferred:
        if c not in df.columns:  # ensure columns exist
            df[c] = pd.NA

    # Save
    df.to_csv(OUT_RAW, index=False)
    df.rename(columns={"provider_id":"ccn"}, inplace=True)  # CCN = CMS Certification Number
    df.to_csv(OUT_CLEAN, index=False)

if __name__ == "__main__":
    main()
