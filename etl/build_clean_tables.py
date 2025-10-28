import pandas as pd
from pathlib import Path
from io import StringIO

RAW = Path("data_raw")
CLEAN = Path("data_clean")
CLEAN.mkdir(exist_ok=True)

def read_csv_robust(path: Path) -> pd.DataFrame:
    """
    Try multiple encodings so CMS/CDC/ACS files don't blow up.
    Returns empty DataFrame if file doesn't exist.
    """
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()

    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(path, dtype=str, low_memory=False, encoding=enc)
        except UnicodeDecodeError:
            continue
        except Exception:
            # file exists but other parse issue, break out later
            pass

    # last-ditch: replace bad bytes
    try:
        raw_bytes = path.read_bytes()
        txt = raw_bytes.decode("utf-8", errors="replace")
        return pd.read_csv(StringIO(txt), dtype=str, low_memory=False)
    except Exception:
        return pd.DataFrame()

def get_first(df: pd.DataFrame, *cands):
    """
    Return the first column name from df that matches any of the
    provided lowercase substrings in order.
    """
    cols = list(df.columns)
    lowmap = {c.lower(): c for c in cols}
    for cand in cands:
        cand = cand.lower()
        # exact match first
        if cand in lowmap:
            return lowmap[cand]
        # then substring match
        for c in cols:
            if cand in c.lower():
                return c
    return None

############################
# 1. COUNTY PROFILE
############################

cdc = read_csv_robust(CLEAN / "cdc_stroke_mortality_county.csv")
acs = read_csv_robust(CLEAN / "acs_uninsured_county.csv")

county_profile = pd.DataFrame()

if not cdc.empty:
    # try to identify key columns in CDC stroke mortality file
    fips_col   = get_first(cdc, "fips", "county_fips", "county_fips_code", "fips code")
    county_col = get_first(cdc, "county name", "county", "county_name")
    state_col  = get_first(cdc, "state", "state_name", "state_abbrev")
    rate_col   = get_first(cdc, "stroke", "ischemic stroke", "stroke death rate", "mortality", "death rate")

    cdc_tmp = cdc.copy()
    rename_map = {}
    if fips_col:   rename_map[fips_col]   = "county_fips"
    if county_col: rename_map[county_col] = "county_name"
    if state_col:  rename_map[state_col]  = "state"
    if rate_col:   rename_map[rate_col]   = "stroke_mortality_rate"

    cdc_tmp = cdc_tmp.rename(columns=rename_map)

    # just keep the columns we care about (drop duplicates if multiple years etc.)
    keep_cols = ["county_fips", "county_name", "state", "stroke_mortality_rate"]
    cdc_tmp = cdc_tmp[[c for c in keep_cols if c in cdc_tmp.columns]].drop_duplicates()

    county_profile = cdc_tmp

if not acs.empty:
    # pull uninsured rate and fips
    acs_tmp = acs.copy()
    fips_col2 = get_first(acs_tmp, "fips", "county_fips", "fips code")
    unins_col = None
    for c in acs_tmp.columns:
        lc = c.lower()
        if "uninsur" in lc and ("rate" in lc or "%" in lc):
            unins_col = c
            break

    rename_map2 = {}
    if fips_col2: rename_map2[fips_col2] = "county_fips"
    if unins_col: rename_map2[unins_col] = "uninsured_rate"

    acs_tmp = acs_tmp.rename(columns=rename_map2)
    acs_tmp = acs_tmp[[c for c in ["county_fips", "uninsured_rate"] if c in acs_tmp.columns]].drop_duplicates()

    if not county_profile.empty and "county_fips" in county_profile.columns and "county_fips" in acs_tmp.columns:
        county_profile = county_profile.merge(acs_tmp, on="county_fips", how="left")
    else:
        # fallback: if CDC data was empty, at least export uninsured info
        county_profile = acs_tmp

# compute a simple "burden index": stroke mortality x uninsured%
if (
    not county_profile.empty
    and "stroke_mortality_rate" in county_profile.columns
    and "uninsured_rate" in county_profile.columns
):
    # Try to coerce both to numeric
    def to_float_series(s):
        return pd.to_numeric(
            s.astype(str).str.replace("%", "", regex=False),
            errors="coerce"
        )

    smr = to_float_series(county_profile["stroke_mortality_rate"])
    unins = to_float_series(county_profile["uninsured_rate"])
    county_profile["burden_index"] = (smr * unins) / 100.0

county_profile.to_csv(CLEAN / "county_profile.csv", index=False)

############################
# 2. HOSPITAL PROFILE
############################

hosp_info = read_csv_robust(CLEAN / "cms_hospital_info.csv")
stroke_out = read_csv_robust(CLEAN / "cms_stroke_outcomes.csv")

hospital_profile = pd.DataFrame()

if not hosp_info.empty:
    hi = hosp_info.copy()

    # find ID and location columns in hospital info
    fac_id_col = (
        get_first(hi, "ccn", "cms certification number", "facility id", "provider id", "facility id ")
        or get_first(hi, "facility id")
    )
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

    hospital_profile = hi

    if not stroke_out.empty:
        so = stroke_out.copy()
        # stroke_out is already filtered to rows where measure mentions "stroke"
        # We want something like hospital-level stroke mortality or readmission score.

        so_fac_col = (
            get_first(so, "facility id", "ccn", "provider id", "cms certification number")
            or get_first(so, "facility_id")
        )
        meas_name_col = get_first(so, "measure name", "measure_name")
        score_col     = get_first(so, "score", "rate", "mortality rate", "death rate")
        comp_nat_col  = get_first(so, "compared to national", "compared to national rate", "compared to national category")

        rename_so = {}
        if so_fac_col:    rename_so[so_fac_col]    = "facility_id"
        if meas_name_col: rename_so[meas_name_col] = "stroke_measure_name"
        if score_col:     rename_so[score_col]     = "stroke_score"
        if comp_nat_col:  rename_so[comp_nat_col]  = "compared_to_national"

        so = so.rename(columns=rename_so)

        keep_so = ["facility_id","stroke_measure_name","stroke_score","compared_to_national"]
        so = so[[c for c in keep_so if c in so.columns]]

        # collapse to one row per facility_id (take first stroke row for now)
        if "facility_id" in so.columns:
            so_summary = (
                so
                .groupby("facility_id", as_index=False)
                .first()
            )

            if "facility_id" in hospital_profile.columns:
                hospital_profile = hospital_profile.merge(
                    so_summary,
                    on="facility_id",
                    how="left"
                )

hospital_profile.to_csv(CLEAN / "hospital_profile.csv", index=False)

print("Done. Wrote:")
print(" - data_clean/county_profile.csv  rows:", len(county_profile))
print(" - data_clean/hospital_profile.csv rows:", len(hospital_profile))
