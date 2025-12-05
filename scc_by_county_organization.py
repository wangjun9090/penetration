import pandas as pd

# 1. Define the standardization/consolidation function (V3)
def standardize_org_name_v3(name):
    """Groups organizations by major parent company using name prefixes."""
    name_upper = name.upper()

    # --- BLUE CROSS BLUE SHIELD CONSOLIDATION (including Anthem/Elevance Health) ---
    if name_upper.startswith('BLUE') or \
       name_upper.startswith('BCBS') or \
       name_upper.startswith('ANTHEM') or \
       'BCBS' in name_upper:
        return 'BLUE CROSS BLUE SHIELD (Consolidated)'

    # --- UNITEDHEALTHCARE CONSOLIDATION ---
    if name_upper.startswith('UNITEDHEALTHCARE') or \
       name_upper.startswith('UNITED HEALTHCARE') or \
       name_upper == 'SIERRA HEALTH AND LIFE INSURANCE COMPANY, INC.':
        return 'UNITEDHEALTHCARE GROUP (Consolidated)'

    # --- OTHER MAJOR GROUPS ---
    if name_upper.startswith('HUMANA'):
        return 'HUMANA GROUP (Consolidated)'

    if name_upper.startswith('AETNA') or 'AETNA' in name_upper:
        return 'AETNA (Consolidated)'

    if name_upper.startswith('KAISER'):
        return 'KAISER PERMANENTE (Consolidated)'

    return name

# 2. Load and clean the dataset
file_name = "SCC_Enrollment_MA_2025_11.csv"
df = pd.read_csv(file_name)

# Handle non-numeric enrollment ('.') and convert to integer
df['Enrolled'] = df['Enrolled'].replace('.', 0).astype(int)

# Drop rows where FIPS Code is NaN, as FIPS is required for unique county identification
df.dropna(subset=['FIPS Code'], inplace=True)

# 3. Apply the consolidation function
df['Grouped Organization Name'] = df['Organization Name'].apply(standardize_org_name_v3)

# 4. Pivot/Reshape the data: One row per county, columns for consolidated organizations
county_enrollment = pd.pivot_table(
    df,
    index=['County', 'State', 'FIPS Code'],
    columns='Grouped Organization Name',
    values='Enrolled',
    aggfunc='sum'
).reset_index()

# Fill NaN values with 0
county_enrollment = county_enrollment.fillna(0)

# List the columns for the key consolidated groups and identifying information
key_enrollment_columns = [
    'County',
    'State',
    'FIPS Code',
    'UNITEDHEALTHCARE GROUP (Consolidated)',
    'HUMANA GROUP (Consolidated)',
    'AETNA (Consolidated)',
    'BLUE CROSS BLUE SHIELD (Consolidated)',
    'KAISER PERMANENTE (Consolidated)',
    'CARE IMPROVEMENT PLUS SOUTH CENTRAL INSURANCE CO.'
]

# 5. Create the final, filtered DataFrame
final_county_enrollment = county_enrollment[key_enrollment_columns]

# Rename columns for clarity
final_county_enrollment.columns = [
    'County',
    'State',
    'FIPS_Code',
    'UNITEDHEALTHCARE_GROUP_ENROLLED',
    'HUMANA_GROUP_ENROLLED',
    'AETNA_GROUP_ENROLLED',
    'BCBS_GROUP_ENROLLED',
    'KAISER_PERMANENTE_GROUP_ENROLLED',
    'CARE_IMPROVEMENT_PLUS_ENROLLED'
]

output_file_filtered = "county_enrollment_by_major_groups_filtered_regenerated.csv"
final_county_enrollment.to_csv(output_file_filtered, index=False)

# 6. Final verification checks
rows_in_file = len(final_county_enrollment)
unique_fips = final_county_enrollment['FIPS_Code'].nunique()

print(f"Total rows in the regenerated file: {rows_in_file}")
print(f"Total unique FIPS Codes: {unique_fips}")
print("First 5 rows of the regenerated data:")
print(final_county_enrollment.head())
