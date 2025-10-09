# etl/fetch_cdc_stroke_county.py
from sodapy import Socrata
import pandas as pd, pathlib

DOMAIN = "data.cdc.gov"
DATASET = "vutr-sfkh"  # Stroke mortality 2019-2021, county/state

OUT = pathlib.Path("data_clean/cdc_stroke_mortality_2019_2021.csv")

def main():
    OUT.parent.mkdir(exist_ok=True)
    client = Socrata(DOMAIN, None, timeout=60)
    rows = client.get(DATASET, limit=200000)  # includes state+county rows
    df = pd.DataFrame.from_records(rows)

    # Keep county-only rows if both 'state' and 'county' FIPS exist
    # CDC uses 'stateabbr' etc.; keep all columns so we can refine later
    df.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()
