# etl/fetch_population_county.py
import pandas as pd
from pathlib import Path
import requests
import os
import sys

OUTPUT = Path("data_clean")
OUTPUT.mkdir(exist_ok=True)

API_URL = "https://api.census.gov/data/2023/acs/acs5"

api_key = os.environ.get("CENSUS_API_KEY")
if not api_key:
    print("ERROR: CENSUS_API_KEY environment variable not set")
    sys.exit(1)

PARAMS = {
    "get": "NAME,B01001_001E",
    "for": "county:*",
    "key": api_key
}

print("Fetching U.S. Census population data...")
try:
    response = requests.get(API_URL, params=PARAMS, timeout=30)
    if not response.ok or "application/json" not in response.headers.get("Content-Type", ""):
        print(f"ERROR: Status {response.status_code}  Content-Type: {response.headers.get('Content-Type')}")
        print(f"ERROR: Response body:\n{response.text[:1000]}")
        sys.exit(1)
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"ERROR: Could not load Census population data: {e}")
    sys.exit(1)

cols = data[0]
rows = data[1:]
df = pd.DataFrame(rows, columns=cols)

df = df.rename(columns={
    "NAME": "county_full_name",
    "B01001_001E": "population",
    "state": "state_fips",
    "county": "county_fips"
})

df["population"] = pd.to_numeric(df["population"], errors="coerce")
df["county_fips"] = df["state_fips"].astype(str).str.zfill(2) + df["county_fips"].astype(str).str.zfill(3)
df.to_csv(OUTPUT / "population_county.csv", index=False)
print("SUCCESS: Created population_county.csv with", len(df), "rows.")
