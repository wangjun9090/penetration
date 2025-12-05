import pandas as pd
import numpy as np
import random
import requests # Required for fetching real data from the Census API
import time     # Required for rate limiting/backoff

# --- 1. MA Data Consolidation Function ---

def standardize_org_name(name):
    """Groups organizations by major parent company for MA enrollment data."""
    name_upper = name.upper()

    # BLUE CROSS BLUE SHIELD CONSOLIDATION
    if name_upper.startswith('BLUE') or \
       name_upper.startswith('BCBS') or \
       name_upper.startswith('ANTHEM') or \
       'BCBS' in name_upper:
        return 'BLUE CROSS BLUE SHIELD (Consolidated)'

    # UNITEDHEALTHCARE CONSOLIDATION
    if name_upper.startswith('UNITEDHEALTHCARE') or \
       name_upper.startswith('UNITED HEALTHCARE') or \
       name_upper == 'SIERRA HEALTH AND LIFE INSURANCE COMPANY, INC.':
        return 'UNITEDHEALTHCARE GROUP (Consolidated)'

    # OTHER MAJOR GROUPS
    if name_upper.startswith('HUMANA'):
        return 'HUMANA GROUP (Consolidated)'

    if name_upper.startswith('AETNA') or 'AETNA' in name_upper:
        return 'AETNA (Consolidated)'

    if name_upper.startswith('KAISER'):
        return 'KAISER PERMANENTE (Consolidated)'

    return 'OTHER/UNGROUPED_MA_ENROLLMENT'

# --- 2. Load and Process MA Data ---

file_name = "SCC_Enrollment_MA_2025_11.csv"
df_ma = pd.read_csv(file_name)

# Clean and prepare FIPS and Enrollment columns
df_ma['Enrolled'] = df_ma['Enrolled'].replace('.', 0).astype(int)
df_ma.dropna(subset=['FIPS Code'], inplace=True)
df_ma['FIPS_Code'] = df_ma['FIPS Code'].astype(int)

# Apply consolidation
df_ma['Grouped Organization Name'] = df_ma['Organization Name'].apply(standardize_org_name)

# Pivot the data: Aggregate enrollment by FIPS Code and Grouped Organization
county_enrollment = pd.pivot_table(
    df_ma,
    index=['County', 'State', 'FIPS_Code'],
    columns='Grouped Organization Name',
    values='Enrolled',
    aggfunc='sum'
).reset_index()

# Fill NaN (where a plan has no enrollment in a county) with 0
county_enrollment = county_enrollment.fillna(0)

# Identify all unique FIPS Codes for the merge
unique_fips = county_enrollment['FIPS_Code'].unique()

print(f"MA Data successfully aggregated for {len(unique_fips)} unique FIPS codes.")


# --- 3. Generate Real US Census Data for Online Usage Correlates ---
def get_census_api_data(fips_list, api_key=None):
    """
    Fetches real county-level demographic data from the US Census API (ACS 5-Year Estimates).

    NOTE: This requires a Census API Key and the 'requests' library.
          The API Key must be obtained from the Census Bureau.

    Variables Mapping (ACS 2022 5-Year Estimates, B06011, B19013, B27010, etc.):
    - Pct_Broadband_Access: C28002_007E (Households w/ Internet, Broadband Subscription) / C28002_001E (Total Households)
    - Median_Household_Income: B19013_001E
    - Pct_Bachelors_Or_Higher: B06009_005E + B06009_006E (Bachelors + Graduate) / B06009_001E (Pop 25+)
    - Pct_Single_Resident_HHLD: B11001_002E (Total Households, 1-person household) / B11001_001E (Total Households)
    - Pct_Pop_Aged_75_Plus: B01001_021E + B01001_022E + B01001_023E + B01001_045E + B01001_046E + B01001_047E (Males/Females 75+) / B01001_001E (Total Pop)
    - Pct_Non_Hispanic_White: B03002_003E / B03002_001E (Total Pop)
    """

    # --- 3A. Census Variables and Labels ---
    # NOTE: Urbanized Population data (Pct_Urbanized_Population) is not easily available
    # via the standard ACS 5-Year API endpoint and often requires separate Geo data.
    # We are omitting it for simplicity in this API call structure.
    # Pct_Dual_Eligible_MA_Members is a CMS metric and must be sourced separately.

    # Primary ACS variables to fetch (raw counts needed to calculate percentages/rates)
    # Using the 2022 5-Year Estimates as the most recent comprehensive data
    CENSUS_VARS = [
        'B19013_001E', # Median Household Income (Already a rate)
        'B06009_005E', 'B06009_006E', 'B06009_001E', # Education (Numerator parts + Denominator)
        'C28002_007E', 'C28002_001E', # Broadband (Numerator + Denominator)
        'B11001_002E', 'B11001_001E', # Single Resident HHLD (Numerator + Denominator)
        'B01001_001E', 'B01001_021E', 'B01001_022E', 'B01001_023E', 'B01001_045E', 'B01001_046E', 'B01001_047E', # 75+ Age (Denominator + Numerator parts)
        'B03002_003E', 'B03002_001E' # Non-Hispanic White (Numerator + Denominator)
    ]

    base_url = "https://api.census.gov/data/2022/acs/acs5"
    variable_string = ','.join(CENSUS_VARS)
    all_data = []

    # --- 3B. Prepare FIPS codes for API calls ---
    # The Census API requires state and county FIPS to be 2 and 3 digits respectively (e.g., '01' + '001')
    fips_to_process = sorted([str(f).zfill(5) for f in fips_list])

    for fips5 in fips_to_process:
        state_fips = fips5[:2]
        county_fips = fips5[2:]

        # --- 3C. Construct API Request URL ---
        # The 'for' parameter requests data for a specific county within a specific state.
        params = {
            'get': variable_string,
            'for': f'county:{county_fips}',
            'in': f'state:{state_fips}',
            'key': api_key # Replace with your actual Census API Key
        }

        # --- 3D. Execute API Request with Retry Logic (Crucial for stability) ---
        max_retries = 3
        delay = 1
        for attempt in range(max_retries):
            try:
                print(f"Fetching data for FIPS {fips5}, attempt {attempt + 1}...")
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                data = response.json()
                # API returns list of lists: [[Variable names, ...], [Value 1, Value 2, ..., state, county]]

                if len(data) > 1:
                    # Create a dictionary mapping variable names to their values
                    header = data[0]
                    row_data = {header[i]: data[1][i] for i in range(len(header))}
                    row_data['FIPS_Code'] = int(fips5)
                    all_data.append(row_data)
                else:
                    print(f"No data returned for FIPS {fips5}.")
                break # Success, exit retry loop
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for FIPS {fips5}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
                    delay *= 2 # Exponential backoff
                else:
                    print(f"Failed to fetch data for FIPS {fips5} after {max_retries} attempts.")
                    # Append an entry with just the FIPS code, others will be NaN
                    all_data.append({'FIPS_Code': int(fips5)})


    # --- 3E. Process Raw Data into Final DataFrame ---
    df_raw = pd.DataFrame(all_data).set_index('FIPS_Code').replace(['', None], np.nan).astype(float)
    df_result = pd.DataFrame(index=df_raw.index)

    # 1. Median Household Income (Already available)
    df_result['Median_Household_Income'] = df_raw['B19013_001E']

    # 2. Pct_Broadband_Access
    df_result['Pct_Broadband_Access'] = (df_raw['C28002_007E'] / df_raw['C28002_001E']) * 100

    # 3. Pct_Bachelors_Or_Higher
    bachelors_plus = df_raw['B06009_005E'] + df_raw['B06009_006E']
    df_result['Pct_Bachelors_Or_Higher'] = (bachelors_plus / df_raw['B06009_001E']) * 100

    # 4. Pct_Single_Resident_HHLD
    df_result['Pct_Single_Resident_HHLD'] = (df_raw['B11001_002E'] / df_raw['B11001_001E']) * 100

    # 5. Pct_Pop_Aged_75_Plus
    pop_75_plus = df_raw[['B01001_021E', 'B01001_022E', 'B01001_023E', 'B01001_045E', 'B01001_046E', 'B01001_047E']].sum(axis=1)
    df_result['Pct_Pop_Aged_75_Plus'] = (pop_75_plus / df_raw['B01001_001E']) * 100

    # 6. Pct_Non_Hispanic_White
    df_result['Pct_Non_Hispanic_White'] = (df_raw['B03002_003E'] / df_raw['B03002_001E']) * 100

    # 7. Dual Eligible (CMS data - cannot be fetched via Census API, so we simulate a proxy)
    # In a real-world scenario, this data would come from CMS, not the Census Bureau.
    # We retain the simulation for this one variable but make it dependent on Income/Age for realism.
    np.random.seed(42) # Ensure reproducible results for this one simulated metric
    # Lower income and higher age generally correlates with higher dual-eligibility
    simulated_dual_eligibility = (
        (100 - df_result['Median_Household_Income'] / 1500) * 0.1 +
        (df_result['Pct_Pop_Aged_75_Plus'] * 0.5) +
        np.random.uniform(5.0, 15.0, size=len(df_result))
    ).clip(5, 40) # Ensure values are within a realistic range
    df_result['Pct_Dual_Eligible_MA_Members'] = simulated_dual_eligibility

    # Ensure all calculated results are capped at 100% and handle division by zero (NaNs)
    df_result = df_result.clip(upper=100.0)
    df_result = df_result.fillna(0) # Fill NaNs that result from missing data or divide-by-zero

    return df_result


# --- 4. Call the Data Fetch Function ---

# NOTE: You MUST replace 'YOUR_CENSUS_API_KEY' with your actual key to make this work.
# If you run this script outside of an environment that supports external HTTP requests
# and the 'requests' library, this section will fail.
CENSUS_API_KEY = 'YOUR_CENSUS_API_KEY'

# IMPORTANT: If the API key is not provided or the network call fails, this will return
# a DataFrame with FIPS codes and zeros/NaNs.
try:
    df_census_real = get_census_api_data(unique_fips, api_key=CENSUS_API_KEY)
except NameError:
    print("\nWARNING: The 'requests' library is not available, using simulated data fallback.")
    # Fallback to the original simulated function if real API fetch fails or is impossible
    def generate_simulated_census_data_fallback(fips_list):
        """Generates synthetic data (Fallback)."""
        random.seed(42)
        simulated_data = {
            'FIPS_Code': fips_list,
            'Median_Household_Income': np.random.randint(35000, 150000, size=len(fips_list)),
            'Pct_Bachelors_Or_Higher': np.random.uniform(5.0, 70.0, size=len(fips_list)),
            'Pct_Broadband_Access': np.random.uniform(60.0, 99.0, size=len(fips_list)),
            'Pct_Single_Resident_HHLD': np.random.uniform(15.0, 45.0, size=len(fips_list)),
            'Pct_Pop_Aged_75_Plus': np.random.uniform(5.0, 20.0, size=len(fips_list)),
            'Pct_Non_Hispanic_White': np.random.uniform(10.0, 99.0, size=len(fips_list)),
            'Pct_Dual_Eligible_MA_Members': np.random.uniform(5.0, 40.0, size=len(fips_list)),
        }
        df_census = pd.DataFrame(simulated_data).set_index('FIPS_Code')
        # Add Pct_Urbanized_Population to match the original simulation structure
        df_census['Pct_Urbanized_Population'] = np.random.uniform(0.0, 100.0, size=len(fips_list))
        return df_census

    # Assign fallback result to the census variable name
    df_census_real = generate_simulated_census_data_fallback(unique_fips)

# Use the result, whether real or simulated
df_census_simulated = df_census_real


# --- 5. Merge the Datasets ---

# Rename MA enrollment columns for cleaner output
ma_columns = {
    'UNITEDHEALTHCARE GROUP (Consolidated)': 'ENR_UNITEDHEALTHCARE',
    'HUMANA GROUP (Consolidated)': 'ENR_HUMANA',
    'AETNA (Consolidated)': 'ENR_AETNA',
    'BLUE CROSS BLUE SHIELD (Consolidated)': 'ENR_BCBS',
    'KAISER PERMANENTE (Consolidated)': 'ENR_KAISER',
    'OTHER/UNGROUPED_MA_ENROLLMENT': 'ENR_OTHER_MA'
}

# Select relevant columns and set FIPS as index for merging
df_ma_final = county_enrollment[['County', 'State', 'FIPS_Code'] + list(ma_columns.keys())]
df_ma_final = df_ma_final.rename(columns=ma_columns)
df_ma_final = df_ma_final.set_index('FIPS_Code')

# Merge the MA data (Left side) with the Census data (Right side)
df_final = df_ma_final.join(df_census_simulated, how='left')
df_final = df_final.reset_index() # Bring FIPS back as a column

# Fill any remaining NaNs (e.g., if a county's data was not available from Census API)
df_final = df_final.fillna(0)

# --- 6. Save the Final Combined Dataset ---

output_file_name = "county_ma_enrollment_with_demographics.csv"
df_final.to_csv(output_file_name, index=False)

print("\n--- Final Output Summary ---")
print(f"Total rows in final file: {len(df_final)}")
print("First 5 rows of the combined data:")
print(df_final.head().to_markdown(index=False))

# Optional: Print correlation check (simulated)
print("\nSimulated Correlation Check (High numbers suggest correlation):")
# Create a dummy 'Online_User_Rate' that is highly correlated with the selected Census vars
df_final['Online_User_Rate_SIMULATED'] = (
    df_final['Pct_Broadband_Access'] * 0.5 +
    (100 - df_final['Pct_Pop_Aged_75_Plus']) * 0.3 + # Inverse relationship with 75+
    df_final['Median_Household_Income'] / 1000 * 0.1
)

# Show correlation between simulated MA enrollment and simulated user rate
print(df_final[['ENR_UNITEDHEALTHCARE', 'ENR_HUMANA', 'ENR_BCBS', 'Online_User_Rate_SIMULATED']].corr().to_markdown())
