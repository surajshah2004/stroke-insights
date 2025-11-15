import pandas as pd
from pathlib import Path
from io import StringIO

RAW = Path("data_raw")
CLEAN = Path("data_clean")
CLEAN.mkdir(exist_ok=True)

########################
# helpers
########################

def read_csv_robust(path: Path) -> pd.DataFrame:
    """
    Try multiple encodings so CMS/CDC/ACS files don't blow up.
    Returns empty DataFrame if file doesn't exist or is tiny.
    """
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()

    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(path, dtype=str, low_memory=False, encoding=enc)
        except UnicodeDecodeError:
            continue
        except Exception:
            # fall through and try fallback below
            pass

    # last-ditch: replace invalid bytes
    try:
        raw_bytes = path.read_bytes()
        txt = raw_bytes.decode("utf-8", errors="replace")
        return pd.read_csv(StringIO(txt), dtype=str, low_memory=False)
    except Exception:
        return pd.DataFrame()


def get_first(df: pd.DataFrame, *cands):
    """
    Return the first df column whose name either equals or contains any of the
    provided candidate strings (case-insensitive). If none, return None.
    """
    if df.empty:
        return None
    cols = list(df.columns)
    lowmap = {c.lower(): c for c in cols}
    # exact match pass
    for cand in cands:
        cand_l = cand.lower()
        if cand_l in lowmap:
            return lowmap[cand_l]
    # substring pass
    for cand in cands:
        cand_l = cand.lower()
        for c in cols:
            if cand_l in c.lower():
                return c
    return None


########################
# 1. Build county_profile (CDC + ACS, explicit columns, one row per county)
########################

cdc = read_csv_robust(CLEAN / "cdc_stroke_mortality_county.csv")
acs = read_csv_robust(CLEAN / "acs_uninsured_county.csv")

county_profile = pd.DataFrame()

# ----- CDC: stroke mortality (latest year, Overall / Overall) -----
if not cdc.empty:
    cdc_tmp = cdc.copy()

    # keep only Overall / Overall and latest year
    # (column names are exactly from your file)
    cdc_tmp = cdc_tmp[
        (cdc_tmp["stratification1"] == "Overall") &
        (cdc_tmp["stratification2"] == "Overall")
    ].copy()

    # pick latest year present in the file
    try:
        cdc_tmp["year_int"] = pd.to_numeric(cdc_tmp["year"], errors="coerce")
        max_year = int(cdc_tmp["year_int"].max())
        cdc_tmp = cdc_tmp[cdc_tmp["year_int"] == max_year].copy()
    except Exception:
        # if something goes wrong, just keep whatever is there
        pass

    # now select just the columns we care about
    cdc_tmp = cdc_tmp[["locationid", "locationdesc", "locationabbr", "data_value"]].copy()
    cdc_tmp = cdc_tmp.rename(
        columns={
            "locationid": "county_fips",
            "locationdesc": "county_name",
            "locationabbr": "state",
            "data_value": "stroke_mortality_rate",
        }
    )

    # ensure 5-digit FIPS
    cdc_tmp["county_fips"] = (
        cdc_tmp["county_fips"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.zfill(5)
    )

    # one row per county_fips (if any duplicates remain, keep first)
    cdc_tmp = cdc_tmp.drop_duplicates(subset=["county_fips"])

    county_profile = cdc_tmp

# ----- ACS: uninsured -----
if not acs.empty:
    acs_tmp = acs[["fips", "pct_uninsured"]].copy()
    acs_tmp = acs_tmp.rename(
        columns={
            "fips": "county_fips",
            "pct_uninsured": "uninsured_rate",
        }
    )
    acs_tmp["county_fips"] = (
        acs_tmp["county_fips"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.zfill(5)
    )
    acs_tmp = acs_tmp.drop_duplicates(subset=["county_fips"])

    if not county_profile.empty:
        county_profile = county_profile.merge(
            acs_tmp, on="county_fips", how="left"
        )
    else:
        county_profile = acs_tmp

# ----- Compute burden_index if we have both pieces -----
if (
    not county_profile.empty
    and "stroke_mortality_rate" in county_profile.columns
    and "uninsured_rate" in county_profile.columns
):
    def to_float_series(s):
        return pd.to_numeric(
            s.astype(str).str.replace("%", "", regex=False),
            errors="coerce",
        )

    smr = to_float_series(county_profile["stroke_mortality_rate"])
    unins = to_float_series(county_profile["uninsured_rate"])
    county_profile["burden_index"] = (smr * unins) / 100.0

county_profile.to_csv(CLEAN / "county_profile.csv", index=False)



########################
# 2. Prepare hospital_info (from CMS raw into clean)
########################

raw_hosp = read_csv_robust(RAW / "cms_hospital_info_export.csv")

if not raw_hosp.empty:
    # attempt to identify expected columns
    hi = raw_hosp.copy()

    fac_id_col = get_first(hi, "ccn", "cms certification number", "facility id", "provider id")
    name_col   = get_first(hi, "hospital name", "provider name", "facility name")
    addr_col   = get_first(hi, "address", "address line 1")
    city_col   = get_first(hi, "city", "city/town")
    state_col  = get_first(hi, "state")
    zip_col    = get_first(hi, "zip", "zip code")
    county_col = get_first(hi, "county", "county/parish")
    phone_col  = get_first(hi, "telephone", "phone")

    rename_hi = {}
    if fac_id_col: rename_hi[fac_id_col] = "facility_id"
    if name_col:   rename_hi[name_col]   = "hospital_name"
    if addr_col:   rename_hi[addr_col]   = "address"
    if city_col:   rename_hi[city_col]   = "city"
    if state_col:  rename_hi[state_col]  = "state"
    if zip_col:    rename_hi[zip_col]    = "zip"
    if county_col: rename_hi[county_col] = "county"
    if phone_col:  rename_hi[phone_col]  = "phone"

    hi = hi.rename(columns=rename_hi)

    keep_hi = ["facility_id","hospital_name","address","city","state","zip","county","phone"]
    hi = hi[[c for c in keep_hi if c in hi.columns]].drop_duplicates()

    # write cleaned hospital info
    hi.to_csv(CLEAN / "cms_hospital_info.csv", index=False)
else:
    # make sure file exists, even if empty
    pd.DataFrame().to_csv(CLEAN / "cms_hospital_info.csv", index=False)


########################
# 3. Prepare stroke_outcomes (from CMS raw into clean)
########################

raw_outcomes = read_csv_robust(RAW / "cms_outcomes_export.csv")

if not raw_outcomes.empty:
    so = raw_outcomes.copy()
    # normalize cols lowercase for searching
    so.columns = [c.strip().lower() for c in so.columns]

    # 3a. Try to explicitly grab MORT_30_STK by Measure ID
    measure_id_col = get_first(so, "measure id", "measure_id")
    mort_mask = None
    if measure_id_col:
        mort_mask = so[measure_id_col].astype(str).str.upper().eq("MORT_30_STK")

    # 3b. Build a broader "stroke" text mask as fallback
    cols_try = [
        "measure_id","measure id","measure_name","measure name",
        "condition","condition name","clinical condition","topic","topic name","category"
    ]
    stroke_text_mask = None
    for c in cols_try:
        if c in so.columns:
            m = so[c].astype(str).str.lower().str.contains("stroke", na=False)
            stroke_text_mask = m if stroke_text_mask is None else (stroke_text_mask | m)

    # If we found any MORT_30_STK rows, use those;
    # otherwise fall back to any "stroke" rows.
    if mort_mask is not None and mort_mask.any():
        mask = mort_mask
    else:
        if stroke_text_mask is None:
            # last-resort: search all text columns for "stroke"
            mask = pd.Series(False, index=so.index)
            for c in so.columns:
                if so[c].dtype == object:
                    mask = mask | so[c].astype(str).str.lower().str.contains("stroke", na=False)
        else:
            mask = stroke_text_mask

    stroke_only = so.loc[mask].copy()

    # rename a few useful columns for merging
    fac_id_col2 = get_first(
        stroke_only,
        "facility id","ccn","provider id","cms certification number","facility_id"
    )
    meas_name_col = get_first(stroke_only, "measure name","measure_name")
    score_col     = get_first(stroke_only, "score","rate","mortality rate","death rate")
    comp_nat_col  = get_first(stroke_only, "compared to national","compared to national rate","compared to national category")

    rename_so = {}
    if fac_id_col2:   rename_so[fac_id_col2]   = "facility_id"
    if meas_name_col: rename_so[meas_name_col] = "stroke_measure_name"
    if score_col:     rename_so[score_col]     = "stroke_score"
    if comp_nat_col:  rename_so[comp_nat_col]  = "compared_to_national"

    stroke_only = stroke_only.rename(columns=rename_so)

    keep_so = ["facility_id","stroke_measure_name","stroke_score","compared_to_national"]
    stroke_only = stroke_only[[c for c in keep_so if c in stroke_only.columns]]

    # collapse to one row per facility_id (take first stroke row for now)
    if "facility_id" in stroke_only.columns and not stroke_only.empty:
        stroke_summary = (
            stroke_only
            .groupby("facility_id", as_index=False)
            .first()
        )
    else:
        stroke_summary = pd.DataFrame()

    # write cleaned stroke outcomes (new + legacy filename)
    stroke_summary.to_csv(CLEAN / "cms_stroke_outcomes.csv", index=False)
    stroke_summary.to_csv(CLEAN / "cms_complications_deaths_stroke.csv", index=False)
else:
    empty_df = pd.DataFrame()
    empty_df.to_csv(CLEAN / "cms_stroke_outcomes.csv", index=False)
    empty_df.to_csv(CLEAN / "cms_complications_deaths_stroke.csv", index=False)
    stroke_summary = empty_df


########################
# 4. Build hospital_profile (hospital info + stroke outcomes)
########################

clean_hosp = read_csv_robust(CLEAN / "cms_hospital_info.csv")
clean_stroke = read_csv_robust(CLEAN / "cms_stroke_outcomes.csv")

hospital_profile = pd.DataFrame()

if not clean_hosp.empty:
    hospital_profile = clean_hosp.copy()
    if not clean_stroke.empty and "facility_id" in clean_stroke.columns and "facility_id" in hospital_profile.columns:
        hospital_profile = hospital_profile.merge(
            clean_stroke,
            on="facility_id",
            how="left"
        )

hospital_profile.to_csv(CLEAN / "hospital_profile.csv", index=False)


########################
# 5. Tiny helper to summarize row counts
########################

def safe_len(path: Path) -> int:
    """
    Try to read a CSV and return number of rows.
    If it's totally empty (0 bytes or no header), return 0 instead of crashing.
    """
    try:
        if not path.exists() or path.stat().st_size == 0:
            return 0
        df = pd.read_csv(path, dtype=str, low_memory=False)
        return len(df)
    except Exception:
        return 0

print("Done. Wrote:")
print(f" - data_clean/county_profile.csv                rows: {safe_len(CLEAN / 'county_profile.csv')}")
print(f" - data_clean/cms_hospital_info.csv             rows: {safe_len(CLEAN / 'cms_hospital_info.csv')}")
print(f" - data_clean/cms_stroke_outcomes.csv           rows: {safe_len(CLEAN / 'cms_stroke_outcomes.csv')}")
print(f" - data_clean/cms_complications_deaths_stroke.csv rows: {safe_len(CLEAN / 'cms_complications_deaths_stroke.csv')}")
print(f" - data_clean/hospital_profile.csv              rows: {safe_len(CLEAN / 'hospital_profile.csv')}")
