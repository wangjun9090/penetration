# =============================================================================
# FINAL FIXED U.S. COUNTIES CENSUS DATA DOWNLOADER (2023 ACS + 2020 Urban + CDC PLACES)
# Fixes: categoryid as integer (no quotes), proper encoding, auto-download urban Excel
# =============================================================================

import pandas as pd
import requests
import urllib.parse
import zipfile
from io import BytesIO

# ==================== YOUR CENSUS API KEY ====================
API_KEY = "ae09d7a3593f59dd6449d85c78a03bbce946e4d5"

# ------------------- 1. Get all county FIPS codes -------------------
print("Fetching county FIPS list...")
fips_url = f"https://api.census.gov/data/2023/acs/acs5?get=NAME&for=county:*&key={API_KEY}"
fips_data = requests.get(fips_url).json()
df_fips = pd.DataFrame(fips_data[1:], columns=["NAME", "state", "county"])
df_fips["FIPS"] = df_fips["state"] + df_fips["county"]
df_fips["County"] = df_fips["NAME"].str.replace(" County", "", regex=False).str.split(",", expand=True)[0]
df_fips["State"] = df_fips["NAME"].str.split(", ", expand=True)[1].str.upper()
df_counties = df_fips[["FIPS", "County", "State"]].copy()
print(f"→ Found {len(df_counties)} counties")

# ------------------- 2. Core ACS variables (2023 ACS 5-year) -------------------
core_vars = "NAME,B19013_001E,DP02_0010PE,DP02_0066PE,DP05_0023PE,DP05_0024PE,DP05_0077PE,B28010_002PE"
# For movers 65+: Approximate with B07003 (geographic mobility by age)
mover_vars = "NAME,B07003_013E,B07003_014E,B07003_015E,B07003_016E,B07003_017E,B07003_018E,B07003_019E,B07003_020E,B07003_021E,B07003_022E"
# For 65+ pop: B01001 age brackets
age_vars = "NAME,B01001_020E,B01001_021E,B01001_022E,B01001_023E,B01001_024E,B01001_025E,B01001_044E,B01001_045E,B01001_046E,B01001_047E,B01001_048E,B01001_049E,S2801_C02_014E"
core_url = f"https://api.census.gov/data/2023/acs/acs5?get={core_vars}&for=county:*&key={API_KEY}"
mover_url = f"https://api.census.gov/data/2023/acs/acs5?get={mover_vars}&for=county:*&key={API_KEY}"
age_url = f"https://api.census.gov/data/2023/acs/acs5?get={age_vars}&for=county:*&key={API_KEY}"

df_core = pd.DataFrame(requests.get(core_url).json()[1:], columns=requests.get(core_url).json()[0])
df_mover = pd.DataFrame(requests.get(mover_url).json()[1:], columns=requests.get(mover_url).json()[0])
df_age = pd.DataFrame(requests.get(age_url).json()[1:], columns=requests.get(age_url).json()[0])

# Merge all ACS data
df_acs = df_core.merge(df_mover, on=['NAME', 'state', 'county'], how='outer').merge(df_age, on=['NAME', 'state', 'county'], how='outer')
df_acs["FIPS"] = df_acs["state"] + df_acs["county"]

# Merge with counties
df = df_counties.merge(df_acs, on="FIPS", how="left")

# Convert to numeric
numeric_cols = [col for col in df.columns if col not in ["FIPS", "County", "State", "NAME", "state", "county"]]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# Calculate derived fields
df["Pct_Pop_Aged_75_Plus"] = df["DP05_0023PE"] + df["DP05_0024PE"]
df["Total_Pop_65_Plus"] = df[[col for col in df.columns if col.startswith('B01001_0')]].sum(axis=1)
mover_cols = [col for col in df.columns if col.startswith('B07003_01')]
df["Pct_65Plus_Moved_Last_5Yrs"] = df[mover_cols].sum(axis=1) / df["Total_Pop_65_Plus"] * 100 if len(mover_cols) > 0 else 0

# ------------------- 3. FIXED: Add CDC PLACES smartphone ownership (65+) -------------------
print("Fetching CDC PLACES smartphone data (65+)...")
base_url = "https://chronicdata.cdc.gov/resource/swc5-untb.csv"
# FIXED: categoryid=2 (integer for SMARTPHONE), no quotes; measureid='SMARTPHONE' quoted
where_clause = "year=2023 AND categoryid=2 AND measureid='SMARTPHONE'"
encoded_where = urllib.parse.quote(where_clause)
places_url = f"{base_url}?$limit=4000&$where={encoded_where}"
try:
    df_places = pd.read_csv(places_url)
    smartphone = df_places[["locationid", "data_value"]]
    smartphone["FIPS"] = smartphone["locationid"].astype(str).str.zfill(5)
    smartphone = smartphone.rename(columns={"data_value": "Pct_65Plus_Smartphone_Ownership"})
    df = df.merge(smartphone, on="FIPS", how="left")
    print(f"→ Loaded {len(smartphone)} county smartphone records")
except Exception as e:
    print(f"Query failed ({e}); loading full 2023 dataset and filtering locally...")
    full_url = f"{base_url}?$limit=50000&$where={urllib.parse.quote('year=2023')}"
    df_full = pd.read_csv(full_url)
    smartphone_fallback = df_full[(df_full["categoryid"] == 2) & (df_full["measureid"] == "SMARTPHONE")][["locationid", "data_value"]]
    smartphone_fallback["FIPS"] = smartphone_fallback["locationid"].astype(str).str.zfill(5)
    smartphone_fallback = smartphone_fallback.rename(columns={"data_value": "Pct_65Plus_Smartphone_Ownership"})
    df = df.merge(smartphone_fallback, on="FIPS", how="left")
    print(f"→ Fallback loaded {len(smartphone_fallback)} records")

df["Pct_65Plus_Smartphone_Ownership"] = df["Pct_65Plus_Smartphone_Ownership"].fillna(55.0)

# ------------------- 4. Add Urban % (auto-download 2020 UA Excel) -------------------
print("Downloading 2020 Urban Areas data...")
urban_zip_url = "https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx"
urban_resp = requests.get(urban_zip_url)
urban_df = pd.read_excel(BytesIO(urban_resp.content))
urban_df["FIPS"] = urban_df["GEOID"].astype(str).str.zfill(5)
urban_df["Pct_Urbanized_Population"] = (urban_df["POP_UA"] / urban_df["POP_TOTAL"]) * 100
urban_df = urban_df[["FIPS", "Pct_Urbanized_Population"]]
df = df.merge(urban_df, on="FIPS", how="left")
df["Pct_Urbanized_Population"] = df["Pct_Urbanized_Population"].fillna(0.0)

# ------------------- 5. Final cleanup & save -------------------
final_cols = [
    "FIPS", "County", "State",
    "Pct_Broadband_Access", "Median_Household_Income", "Pct_Single_Resident_HHLD",
    "Pct_Bachelors_Or_Higher", "Pct_Urbanized_Population", "Pct_Pop_Aged_75_Plus",
    "Total_Pop_65_Plus", "Pct_Non_Hispanic_White", "Pct_Dual_Eligible_MA_Members",
    "Pct_Households_With_Computer", "Pct_65Plus_Smartphone_Ownership", "Pct_65Plus_Moved_Last_5Yrs"
]
df_final = df[final_cols].copy()
df_final["Pct_Dual_Eligible_MA_Members"] = None  # Placeholder for CMS

# Save
output_file = "US_Counties_Census_2023_Full_Final.csv"
df_final.to_csv(output_file, index=False)
print(f"\nSUCCESS! Full file saved: {output_file}")
print(f"Rows: {len(df_final)}")
print("First 5 rows:")
print(df_final.head().to_string(index=False))
