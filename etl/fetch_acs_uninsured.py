# etl/fetch_acs_uninsured.py
import requests, pandas as pd, pathlib

YEAR = "2023"  # latest 5-year at time of writing
BASE = f"https://api.census.gov/data/{YEAR}/acs/acs5/subject"
VARS = ["NAME","S2701_C05_001E"]  # Percent Uninsured, civilian noninstitutionalized pop

OUT = pathlib.Path("data_clean/acs_uninsured_county.csv")

def main():
    OUT.parent.mkdir(exist_ok=True)
    params = {"get": ",".join(VARS), "for": "county:*"}
    r = requests.get(BASE, params=params, timeout=60)
    r.raise_for_status()
    rows = r.json()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    # Make a single county FIPS code for easy joins later
    df["state_fips"] = df["state"]
    df["county_fips"] = df["county"]
    df["fips"] = df["state_fips"].str.zfill(2) + df["county_fips"].str.zfill(3)
    df.rename(columns={"S2701_C05_001E":"pct_uninsured"}, inplace=True)
    df.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()
