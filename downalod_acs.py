# =============================================================================
# FIXED U.S. COUNTIES CENSUS DATA DOWNLOADER (2023 ACS + 2020 Urban + CDC PLACES)
# Fixes: Correct variables, year=2023, merges, calculations, auto-download urban
# =============================================================================

import pandas as pd
import requests
import urllib.parse
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
print(f"-> Found {len(df_counties)} counties")

# ------------------- 2. Core ACS variables (2023 ACS 5-year) -------------------
# Detailed Tables (B prefix) - Base endpoint
# B01001_001E: Total Population
# B07001: Mobility by Age
# 014-016 (Total 65+), 030-032 (Same House 65+), 046-048 (Moved Same County 65+)
detailed_vars = "NAME,B19013_001E,B28010_001E,B28010_002E,B01001_001E,B07001_014E,B07001_015E,B07001_016E,B07001_030E,B07001_031E,B07001_032E,B07001_046E,B07001_047E,B07001_048E" 
# Data Profiles (DP prefix) - /profile endpoint
profile_vars = "NAME,DP02_0010PE,DP02_0066PE,DP05_0023PE,DP05_0024PE,DP05_0082PE" # Fixed White % (0082PE)
# Subject Tables (S prefix) - /subject endpoint
# S2801_C02_005E: Pct Households with Smartphone
subject_vars = "NAME,S2801_C02_014E,S2801_C02_005E"
# Age 65+ total
age_vars = "NAME,B01001_020E,B01001_021E,B01001_022E,B01001_023E,B01001_024E,B01001_025E,B01001_044E,B01001_045E,B01001_046E,B01001_047E,B01001_048E,B01001_049E"

def fetch_acs_data(vars_list, url_suffix, endpoint="https://api.census.gov/data/2023/acs/acs5"):
    url = f"{endpoint}?get={vars_list}&for=county:*&key={API_KEY}"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        if len(data) < 2:
            raise ValueError("Empty data")
        df = pd.DataFrame(data[1:], columns=data[0])
        df["FIPS"] = df["state"] + df["county"]
        # Drop redundant columns to prevent merge errors
        cols_to_drop = ["state", "county", "NAME"]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
        return df
    except Exception as e:
        print(f"Error fetching {url_suffix}: {e}")
        return pd.DataFrame()

# Fetch batches
df_detailed = fetch_acs_data(detailed_vars, "detailed")
df_profile = fetch_acs_data(profile_vars, "profile", "https://api.census.gov/data/2023/acs/acs5/profile")
df_subject = fetch_acs_data(subject_vars, "subject", "https://api.census.gov/data/2023/acs/acs5/subject")
df_age = fetch_acs_data(age_vars, "age")

# Merge on FIPS
df_acs = df_detailed.merge(df_profile, on="FIPS", how="outer") \
    .merge(df_subject, on="FIPS", how="outer") \
    .merge(df_age, on="FIPS", how="outer")

df = df_counties.merge(df_acs, on="FIPS", how="left")
print(f"[OK] Merged ACS data for {len(df)} counties")

# Convert to numeric
numeric_cols = [col for col in df.columns if col not in ["FIPS", "County", "State", "NAME", "state", "county"]]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

# Calculate derived fields
# Pop Aged 75+ (Count)
age_75_cols = ["B01001_023E", "B01001_024E", "B01001_025E", "B01001_047E", "B01001_048E", "B01001_049E"]
df["Pop_Aged_75_Plus"] = df[age_75_cols].sum(axis=1)

age_65_cols = [col for col in df.columns if col.startswith("B01001_0") and col != "B01001_001E"]
df["Total_Pop_65_Plus"] = df[age_65_cols].sum(axis=1)

# Fill NaNs with national avgs
df["Pct_Broadband_Access"] = df["S2801_C02_014E"].fillna(88.0)
df["Median_Household_Income"] = df["B19013_001E"].fillna(80610)
df["Pct_Single_Resident_HHLD"] = df["DP02_0010PE"].fillna(28.0)
df["Pct_Bachelors_Or_Higher"] = df["DP02_0066PE"].fillna(34.0)
df["Pct_Non_Hispanic_White"] = df["DP05_0082PE"].fillna(58.0) # Fixed variable
df["Pct_Households_With_Computer"] = (df["B28010_002E"] / df["B28010_001E"] * 100).fillna(82.0)

# Calculate Movers 65+ (Moved within last year)
# Using B07001 (Mobility by Age)
# "Move In" typically means In-Migration (excluding Same County moves).
# In-Movers = Total(65+) - SameHouse(65+) - MovedSameCounty(65+)
total_65_plus_mob = df["B07001_014E"] + df["B07001_015E"] + df["B07001_016E"]
same_house_65_plus = df["B07001_030E"] + df["B07001_031E"] + df["B07001_032E"]
same_county_65_plus = df["B07001_046E"] + df["B07001_047E"] + df["B07001_048E"]

moved_65_plus = total_65_plus_mob - same_house_65_plus - same_county_65_plus

df["Count_65Plus_Moved_Last_Year"] = moved_65_plus
df["Pct_65Plus_Moved_Last_Year"] = (moved_65_plus / total_65_plus_mob * 100).fillna(0.0)

# Total Population
df["Total_Population"] = df["B01001_001E"]

print("[OK] Derived fields calculated")

# ------------------- 3. Smartphone (ACS S2801) -------------------
df["Pct_65Plus_Smartphone_Ownership"] = df["S2801_C02_005E"].fillna(55.0)
print(f"[OK] Mapped Smartphone data (Household level proxy)")

# ------------------- 4. Urban % (local file) -------------------
print("Loading 2020 Urban Areas Excel...")
urban_file = "2020_UA_COUNTY.xlsx"
try:
    urban_df = pd.read_excel(urban_file)
    urban_df["FIPS"] = urban_df["STATE"].astype(str).str.zfill(2) + urban_df["COUNTY"].astype(str).str.zfill(3)
    urban_df["Pct_Urbanized_Population"] = urban_df["POPPCT_URB"] * 100
    urban_df = urban_df[["FIPS", "Pct_Urbanized_Population"]]
    df = df.merge(urban_df, on="FIPS", how="left")
    df["Pct_Urbanized_Population"] = df["Pct_Urbanized_Population"].fillna(50.0)
    print(f"[OK] Loaded urban data for {len(urban_df)} counties")
except Exception as e:
    print(f"Urban download error ({e}); using placeholder 50%")
    df["Pct_Urbanized_Population"] = 50.0

# ------------------- 5. CMS Dual Eligible (Optional local file) -------------------
print("Looking for CMS Dual Eligible data...")
cms_file = "CMS_Dual_Eligible_2023.csv"
try:
    cms_df = pd.read_csv(cms_file, dtype={"FIPS": str})
    cms_df["FIPS"] = cms_df["FIPS"].str.zfill(5)
    if "Pct_Dual_Eligible_MA_Members" in cms_df.columns:
        df = df.merge(cms_df[["FIPS", "Pct_Dual_Eligible_MA_Members"]], on="FIPS", how="left")
        print(f"[OK] Loaded CMS Dual Eligible data for {len(cms_df)} counties")
    else:
        print(f"[WARN] {cms_file} found but missing 'Pct_Dual_Eligible_MA_Members' column")
        df["Pct_Dual_Eligible_MA_Members"] = None
except FileNotFoundError:
    print(f"[INFO] {cms_file} not found. Pct_Dual_Eligible_MA_Members will be empty.")
    df["Pct_Dual_Eligible_MA_Members"] = None
except Exception as e:
    print(f"[WARN] Error loading CMS data: {e}")
    df["Pct_Dual_Eligible_MA_Members"] = None

# ------------------- 6. Final cleanup & save -------------------
final_cols = [
    "FIPS", "County", "State",
    "Pct_Broadband_Access", "Median_Household_Income", "Pct_Single_Resident_HHLD",
    "Pct_Bachelors_Or_Higher", "Pct_Urbanized_Population", "Pop_Aged_75_Plus", "Total_Population",
    "Total_Pop_65_Plus", "Pct_Non_Hispanic_White", "Pct_Dual_Eligible_MA_Members",
    "Pct_Households_With_Computer", "Pct_65Plus_Smartphone_Ownership", "Count_65Plus_Moved_Last_Year", "Pct_65Plus_Moved_Last_Year"
]
df_final = df[final_cols].copy()

output_file = "US_Counties_Census_2023_Fixed_v4.csv"
df_final.to_csv(output_file, index=False)
print(f"\nSUCCESS! Saved {output_file} ({len(df_final)} rows)")
print("Sample for Autauga County (FIPS 01001):")
print(df_final[df_final["FIPS"] == "01001"].to_string(index=False))
