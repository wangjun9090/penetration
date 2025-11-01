import requests
import pandas as pd

API_KEY = "YOUR_ACS_API_KEY_HERE"   # replace with your key
YEAR = 2023                         # most recent 5-year dataset

# ---------- Helper to pull data ----------
def get_acs_table(dataset, table_id, geo="county:*"):
    url = f"https://api.census.gov/data/{YEAR}/{dataset}"
    # Step 1: discover all variables for this table
    vars_url = f"{url}/variables.json"
    variables = requests.get(vars_url).json()["variables"]
    selected_vars = [v for v in variables.keys() if v.startswith(table_id)]
    if "NAME" not in selected_vars:
        selected_vars.insert(0, "NAME")

    # Step 2: fetch table data
    params = {
        "get": ",".join(selected_vars),
        "for": geo,
        "key": API_KEY
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

# ---------- Download three subject tables ----------
print("Downloading S0103 (65+ Population)...")
df_s0103 = get_acs_table("acs/acs5/subject", "S0103")

print("Downloading S1501 (Education)...")
df_s1501 = get_acs_table("acs/acs5/subject", "S1501")

print("Downloading S1903 (Income)...")
df_s1903 = get_acs_table("acs/acs5/subject", "S1903")

# ---------- Merge by county ----------
for df in [df_s1501, df_s1903]:
    for col in ["state", "county"]:
        df[col] = df[col].astype(str)
df_s0103["GEO_ID"] = df_s0103["state"] + df_s0103["county"]

merged = df_s0103.merge(df_s1501, on=["state", "county"], suffixes=("_S0103", "_S1501"))
merged = merged.merge(df_s1903, on=["state", "county"], suffixes=("", "_S1903"))

# ---------- Also pull B01001_020E for reference ----------
print("Downloading B01001_020E (Male 65–66 years)...")
b01001_vars = ["NAME", "B01001_020E"]
params = {
    "get": ",".join(b01001_vars),
    "for": "county:*",
    "key": API_KEY
}
resp = requests.get(f"https://api.census.gov/data/{YEAR}/acs/acs5", params=params)
resp.raise_for_status()
b01001_df = pd.DataFrame(resp.json()[1:], columns=resp.json()[0])

# Merge it in
b01001_df["GEO_ID"] = b01001_df["state"] + b01001_df["county"]
merged = merged.merge(b01001_df[["GEO_ID", "B01001_020E"]], on="GEO_ID", how="left")

# ---------- Save ----------
merged.to_csv("acs_2023_county_65plus_income_edu.csv", index=False)
print("✅ Saved county-level ACS file: acs_2023_county_65plus_income_edu.csv")
