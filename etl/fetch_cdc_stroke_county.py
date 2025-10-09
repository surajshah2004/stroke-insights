from sodapy import Socrata
import pandas as pd, pathlib

DOMAIN = "data.cdc.gov"
PREFERRED = "cpdh-8cna"  # 2021–2023 county
FALLBACK  = "vutr-sfkh"  # 2019–2021 county

OUT = pathlib.Path("data_clean/cdc_stroke_mortality_county.csv")

def fetch(ds):
    client = Socrata(DOMAIN, None, timeout=60)
    rows = client.get(ds, limit=200000)
    return pd.DataFrame.from_records(rows)

def main():
    OUT.parent.mkdir(exist_ok=True)
    try:
        df = fetch(PREFERRED)
    except Exception:
        df = fetch(FALLBACK)
    df.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()

