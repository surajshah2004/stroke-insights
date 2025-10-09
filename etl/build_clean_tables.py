import json, pathlib, pandas as pd
DATA = pathlib.Path("data_clean"); DATA.mkdir(exist_ok=True)

def read(path): 
    p = DATA / path
    return pd.read_csv(p, dtype=str, low_memory=False) if p.exists() else None

def num(s): return pd.to_numeric(s, errors="coerce")

def build_hospital_profile():
    out = DATA / "hospital_profile.csv"
    o = read("cms_stroke_outcomes.csv")
    h = read("cms_hospital_info.csv")
    if o is None or h is None:
        pd.DataFrame().to_csv(out, index=False); return

    if "ccn" not in o.columns and "provider_id" in o.columns: o = o.rename(columns={"provider_id":"ccn"})
    if "ccn" not in h.columns and "provider_id" in h.columns: h = h.rename(columns={"provider_id":"ccn"})

    keep_ids = {"MORT_30_STK":"mortality_30d","READM_30_STK":"readmit_30d"}
    o = o[o["measure_id"].isin(keep_ids)].copy()
    if "end_date" in o.columns:
        o["end_date_dt"] = pd.to_datetime(o["end_date"], errors="coerce")
        o = o.sort_values(["ccn","measure_id","end_date_dt"]).drop_duplicates(["ccn","measure_id"], keep="last")
    o["score_num"] = num(o.get("score"))
    wide = (o.pivot_table(index="ccn", columns="measure_id", values="score_num", aggfunc="first")
              .rename(columns=keep_ids).reset_index())

    h2 = h.copy()
    for c in ["hospital_name","address","city","state","zip_code","county_name","phone_number"]:
        if c not in h2.columns: h2[c] = pd.NA

    # lat/lon from 'location' if present
    h2["lat"], h2["lon"] = pd.NA, pd.NA
    if "location" in h2.columns:
        def latlon(v):
            try:
                v = json.loads(v) if isinstance(v,str) else v
                if isinstance(v,dict):
                    if "latitude" in v and "longitude" in v: return float(v["latitude"]), float(v["longitude"])
                    if "coordinates" in v: return float(v["coordinates"][1]), float(v["coordinates"][0])
            except: pass
            return (pd.NA, pd.NA)
        pairs = h2["location"].apply(latlon)
        h2["lat"] = pairs.apply(lambda t: t[0]); h2["lon"] = pairs.apply(lambda t: t[1])

    prof = h2.merge(wide, on="ccn", how="left")
    for c in ["mortality_30d","readmit_30d","lat","lon"]:
        if c in prof.columns: prof[c] = num(prof[c])
    prof.to_csv(out, index=False)

def build_county_profile():
    out = DATA / "county_profile.csv"
    cdc = read("cdc_stroke_mortality_county.csv")
    acs = read("acs_uninsured_county.csv")
    if cdc is None: pd.DataFrame().to_csv(out, index=False); return

    df = cdc.copy()
    # build FIPS
    fips = None
    for a,b in [("state_fips","county_fips"),("state","county")]:
        if a in df.columns and b in df.columns:
            fips = df[a].astype(str).str.zfill(2)+df[b].astype(str).str.zfill(3); break
    if "fips" in df.columns: fips = df["fips"].astype(str).str.zfill(5) if fips is None else fips
    if fips is None: df.to_csv(out, index=False); return
    df["fips"] = fips

    if acs is not None and "fips" in acs.columns:
        df = df.merge(acs[["fips","pct_uninsured"]], on="fips", how="left")
    else:
        df["pct_uninsured"] = pd.NA
    df.to_csv(out, index=False)

if __name__ == "__main__":
    build_hospital_profile()
    build_county_profile()
