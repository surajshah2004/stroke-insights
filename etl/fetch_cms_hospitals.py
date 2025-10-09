# etl/fetch_cms_hospitals.py
from sodapy import Socrata
import pandas as pd, pathlib

DOMAIN = "data.cms.gov"
DATASET = "xubh-q36u"  # Hospital General Information

OUT_RAW  = pathlib.Path("data_raw/cms_hospital_info.csv")
OUT_CLEAN = pathlib.Path("data_clean/cms_hospital_info.csv")

def main():
    OUT_RAW.parent.mkdir(exist_ok=True)
    OUT_CLEAN.parent.mkdir(exist_ok=True)

    client = Socrata(DOMAIN, None, timeout=60)
    rows = client.get(DATASET, limit=100000)
    df = pd.DataFrame.from_records(rows)

    # Normalize common columns if present
    # Typical columns: provider_id (CCN), hospital_name, address, city, state, zip_code, county_name, phone_number, location
    rename_map = {"provider_id":"ccn"}
    for k,v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k:v}, inplace=True)

    df.to_csv(OUT_RAW, index=False)
    df.to_csv(OUT_CLEAN, index=False)

if __name__ == "__main__":
    main()
