import pandas as pd
from pathlib import Path
import requests

OUTPUT = Path("data_clean")
OUTPUT.mkdir(exist_ok=True)

API_URL = "https://api.census.gov/data/2023/acs/acs5"
PARAMS = {
    "get": "NAME,B01001_001E",
    "for": "county:*"
}

print("Fetching U.S. Census population data...")

try:
    response = requests.get(API_URL, params=PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"ERROR: Could not load Census population data: {e}")
    exit(1)

# Convert to DataFrame
cols = data[0]
rows = data[1:]
df = pd.DataFrame(rows, columns=cols)

# Standardize
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
