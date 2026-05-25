# etl/fetch_acs_uninsured.py
import requests, pandas as pd, pathlib, sys, os

YEAR = "2023"
BASE = f"https://api.census.gov/data/{YEAR}/acs/acs5/subject"
VARS = ["NAME", "S2701_C04_001E"]
OUT  = pathlib.Path("data_clean/acs_uninsured_county.csv")

def main():
    OUT.parent.mkdir(exist_ok=True)

    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        print("[ERROR] CENSUS_API_KEY environment variable not set")
        sys.exit(1)

    params = {"get": ",".join(VARS), "for": "county:*", "key": api_key}
    r = requests.get(BASE, params=params, timeout=60)

    if not r.ok or "application/json" not in r.headers.get("Content-Type", ""):
        print(f"[ERROR] Status {r.status_code}  Content-Type: {r.headers.get('Content-Type')}")
        print(f"[ERROR] Response body:\n{r.text[:1000]}")
        sys.exit(1)

    r.raise_for_status()
    rows = r.json()

    df = pd.DataFrame(rows[1:], columns=rows[0])
    df["fips"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)
    df.rename(columns={"S2701_C04_001E": "pct_uninsured"}, inplace=True)
    df.to_csv(OUT, index=False)
    print(f"[OK] Wrote {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()
