import pandas as pd

# 1. Load the dataset
file_name = "SCC_Enrollment_MA_2025_11.csv"
df = pd.read_csv(file_name)

# 2. Data Cleaning/Conversion: Handle non-numeric enrollment ('.') and convert to integer
# '.' indicates suppressed enrollment (<= 10 enrollees)
df['Enrolled'] = df['Enrolled'].replace('.', 0).astype(int)

# 3. Define the function to standardize and consolidate Organization Names
def standardize_org_name_v3(name):
    """Groups organizations by major parent company using name prefixes."""
    name_upper = name.upper()

    # --- BLUE CROSS BLUE SHIELD CONSOLIDATION (including Anthem/Elevance Health) ---
    # Groups names starting with 'BLUE', 'BCBS', 'ANTHEM', or containing 'BCBS'
    if name_upper.startswith('BLUE') or \
       name_upper.startswith('BCBS') or \
       name_upper.startswith('ANTHEM') or \
       'BCBS' in name_upper:
        return 'BLUE CROSS BLUE SHIELD (Consolidated)'

    # --- UNITEDHEALTHCARE CONSOLIDATION ---
    # Groups names starting with 'UNITEDHEALTHCARE'/'UNITED HEALTHCARE' and known large subsidiaries
    if name_upper.startswith('UNITEDHEALTHCARE') or \
       name_upper.startswith('UNITED HEALTHCARE') or \
       name_upper == 'SIERRA HEALTH AND LIFE INSURANCE COMPANY, INC.':
        return 'UNITEDHEALTHCARE GROUP (Consolidated)'

    # --- OTHER MAJOR GROUPS ---
    if name_upper.startswith('HUMANA'):
        return 'HUMANA GROUP (Consolidated)'

    # Includes entities starting with AETNA or having AETNA in the name
    if name_upper.startswith('AETNA') or 'AETNA' in name_upper:
        return 'AETNA (Consolidated)'

    if name_upper.startswith('KAISER'):
        return 'KAISER PERMANENTE (Consolidated)'

    # Default to original name
    return name

# 4. Apply the standardization function to create a new grouping column
df['Grouped Organization Name'] = df['Organization Name'].apply(standardize_org_name_v3)

# 5. Aggregation: Group by the new column and sum the total enrollment
enrollment_by_grouped_org = df.groupby('Grouped Organization Name')['Enrolled'].sum().reset_index()
enrollment_by_grouped_org.rename(columns={'Enrolled': 'Total Enrolled'}, inplace=True)

# 6. Sort and Select: Sort in descending order and select the top 10
top_10_grouped_enrollment_v3 = enrollment_by_grouped_org.sort_values(by='Total Enrolled', ascending=False).head(10)

# 7. Save the results to the specified CSV file
top_10_grouped_enrollment_v3.to_csv("top_10_ma_enrollment_by_major_groups_final.csv", index=False)

print("File 'top_10_ma_enrollment_by_major_groups_final.csv' has been generated.")
