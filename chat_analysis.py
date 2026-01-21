import pandas as pd
import requests
import io
import time

# ==================== CONFIGURATION ====================
API_KEY = "ae09d7a3593f59dd6449d85c78a03bbce946e4d5"
YEAR = "2023"
DATASET = "acs/acs5"
BASE_URL = f"https://api.census.gov/data/{YEAR}/{DATASET}"

def load_user_data():
    csv_file = 'chat_call_data.csv'
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Error: {csv_file} not found. Please ensure it is in the same directory.")
        return pd.DataFrame()
        
    df['Zip Code'] = df['Zip Code'].astype(str).str.zfill(5)
    df['Share'] = df['Share'].str.rstrip('%').astype(float)
    return df

# ==================== VARIABLE MAPPING ====================
VAR_MAP = {
    'B01001_001E': 'Total_Pop',
    
    # --- 1. Senior Counts (65+) ---
    # Males
    'B01001_020E': 'M_65_66', 'B01001_021E': 'M_67_69', 'B01001_022E': 'M_70_74',
    'B01001_023E': 'M_75_79', 'B01001_024E': 'M_80_84', 'B01001_025E': 'M_85+',
    # Females
    'B01001_044E': 'F_65_66', 'B01001_045E': 'F_67_69', 'B01001_046E': 'F_70_74',
    'B01001_047E': 'F_75_79', 'B01001_048E': 'F_80_84', 'B01001_049E': 'F_85+',

    # --- 2. Senior Race/Ethnicity (65+) --- 
    # Approximating using Total Pop Race because 65+ race tables (B01001A-I) are complex/sparse
    'B01001H_001E': 'Pop_White_NonHisp',
    'B01001B_001E': 'Pop_Black',
    'B01001D_001E': 'Pop_Asian',
    'B01001I_001E': 'Pop_Hispanic',
    
    # --- 3. Economics ---
    'B19049_005E': 'Med_Inc_Senior_HH',  # Household income, householder 65+
    # Poverty 65+ (B17001)
    'B17001_015E': 'Pov_M_65_74', 'B17001_016E': 'Pov_M_75+',
    'B17001_029E': 'Pov_F_65_74', 'B17001_030E': 'Pov_F_75+',
    
    # --- 4. Housing & Wealth ---
    'B25077_001E': 'Med_Home_Value',
    'B25064_001E': 'Med_Gross_Rent',
    'B25035_001E': 'Med_Year_Built',
    # Rent Burden
    'B25070_007E': 'Rent_30_35', 'B25070_008E': 'Rent_35_39', 
    'B25070_009E': 'Rent_40_49', 'B25070_010E': 'Rent_50+',
    'B25070_001E': 'Renters_Total',

    # --- 5. Education (65+) ---
    # B15001 (18-24, 25-34, 35-44, 45-64, 65+)
    # Male 65+: 043 Total. Bachelors 050, Mast 051, Prof 052, Doc 053
    'B15001_050E': 'Edu_M_Bach', 'B15001_051E': 'Edu_M_Mast', 'B15001_052E': 'Edu_M_Prof', 'B15001_053E': 'Edu_M_Doc',
    # Female 65+: 084 Total. Bach 091, Mast 092, Prof 093, Doc 094
    'B15001_091E': 'Edu_F_Bach', 'B15001_092E': 'Edu_F_Mast', 'B15001_093E': 'Edu_F_Prof', 'B15001_094E': 'Edu_F_Doc',

    # --- 6. Tech (65+) ---
    'B28005_014E': 'Tech_Pop_65',
    'B28005_017E': 'Tech_Bb_65',   # With Broadband
    'B28005_018E': 'Tech_None_65', # No Computer
    
    # --- 7. Social / Isolation (65+) ---
    # B09020
    'B09020_021E': 'Alone_M_65', # Male living alone
    'B09020_027E': 'Alone_F_65', # Female living alone
    # B10051 (Grandparents)
    'B10051_004E': 'Grandparent_Resp_For_Kids', # Grandparent householders responsible for grandkids
    
    # --- 8. Disability (65+) ---
    'B18101_016E': 'Dis_M_65_74', 'B18101_019E': 'Dis_M_75+',
    'B18101_035E': 'Dis_F_65_74', 'B18101_038E': 'Dis_F_75+',

    # --- 9. Migration (65+) B07001 ---
    # Moved in last year
    'B07001_014E': 'Mob_65_74', 'B07001_015E': 'Mob_75+', # Totals
    'B07001_030E': 'Mob_Same_House_65_74', 'B07001_031E': 'Mob_Same_House_75+', # Non-movers
    
    # --- 10. Veteran Status (65+) B21001 ---
    'B21001_020E': 'Vet_M_65_74', 'B21001_021E': 'Vet_M_75+',
    'B21001_038E': 'Vet_F_65_74', 'B21001_039E': 'Vet_F_75+',
    
    # --- 11. Employment (65+) B23004 ---
    # E.g. Worked in last 12 months/In Labor force
    # B23004: 
    # Male 65-69 In LF (020), 70-74 (025), 75+ (030)
    # Female 65-69 In LF (046), 70-74 (051), 75+ (056)
    'B23004_020E': 'Emp_M_65_69', 'B23004_025E': 'Emp_M_70_74', 'B23004_030E': 'Emp_M_75+',
    'B23004_046E': 'Emp_F_65_69', 'B23004_051E': 'Emp_F_70_74', 'B23004_056E': 'Emp_F_75+',
}

def fetch_census_data(zip_codes):
    all_vars = list(VAR_MAP.keys())
    chunk_size = 20
    data_chunks = []
    
    # 1. Helper to fetch a list of vars
    def fetch_batch(v_list):
        get_str = ",".join(v_list)
        zip_str = ",".join(zip_codes)
        url = f"{BASE_URL}?get={get_str}&for=zip code tabulation area:{zip_str}&key={API_KEY}"
        r = requests.get(url)
        if r.status_code == 200:
            d = r.json()
            return pd.DataFrame(d[1:], columns=d[0])
        return None

    # 2. Loop chunks
    for i in range(0, len(all_vars), chunk_size):
        chunk_vars = all_vars[i:i+chunk_size]
        
        # Try batch
        df = fetch_batch(chunk_vars)
        if df is not None:
            data_chunks.append(df)
        else:
            print(f"Batch {i} failed. Retrying individual variables...")
            # Retry individually
            for v in chunk_vars:
                single_df = fetch_batch([v])
                if single_df is not None:
                    data_chunks.append(single_df)
                else:
                    print(f"  Variable {v} failed (400/404). Skipping.")
            
    if not data_chunks: return pd.DataFrame()
    
    main_df = data_chunks[0]
    for df in data_chunks[1:]:
        if not df.empty and 'zip code tabulation area' in df.columns:
            # Merge carefully
            main_df = pd.merge(main_df, df, on=['zip code tabulation area'], how='outer')
            main_df = main_df.loc[:, ~main_df.columns.duplicated()]
        
    return main_df

def process_data(df):
    # Ensure all expected raw variables exist (fill missing with 0)
    for raw_code in VAR_MAP.keys():
        if raw_code not in df.columns:
            df[raw_code] = 0

    df = df.rename(columns=VAR_MAP)
    df = df.rename(columns={'zip code tabulation area': 'Zip Code'})
    
    cols = [c for c in df.columns if c != 'Zip Code']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    
    # 1. Senior Pop & Pct
    m_senior = df[['M_65_66','M_67_69','M_70_74','M_75_79','M_80_84','M_85+']].sum(axis=1)
    f_senior = df[['F_65_66','F_67_69','F_70_74','F_75_79','F_80_84','F_85+']].sum(axis=1)
    df['Senior_Pop'] = m_senior + f_senior
    df['Pct_Population_65+'] = df['Senior_Pop'] / df['Total_Pop'] * 100
    
    # 2. Race/Ethnicity (Total Pop)
    # Note: Using total pop as base for race composition of the area
    df['Pct_White_NonHisp'] = df['Pop_White_NonHisp'] / df['Total_Pop'] * 100
    df['Pct_Black'] = df['Pop_Black'] / df['Total_Pop'] * 100
    df['Pct_Asian'] = df['Pop_Asian'] / df['Total_Pop'] * 100
    df['Pct_Hispanic'] = df['Pop_Hispanic'] / df['Total_Pop'] * 100
    
    # 3. Senior Poverty
    pov = df[['Pov_M_65_74','Pov_M_75+','Pov_F_65_74','Pov_F_75+']].sum(axis=1)
    df['Pct_Senior_Poverty'] = pov / df['Senior_Pop'] * 100
    
    # 4. Senior Education
    bach = df[['Edu_M_Bach','Edu_M_Mast','Edu_M_Prof','Edu_M_Doc',
               'Edu_F_Bach','Edu_F_Mast','Edu_F_Prof','Edu_F_Doc']].sum(axis=1)
    df['Pct_Senior_Bach+'] = bach / df['Senior_Pop'] * 100
    
    # 5. Senior Tech
    df['Pct_Senior_Broadband'] = df['Tech_Bb_65'] / df['Tech_Pop_65'] * 100
    df['Pct_Senior_No_Comp'] = df['Tech_None_65'] / df['Tech_Pop_65'] * 100
    
    # 6. Social Isolation
    alone = df['Alone_M_65'] + df['Alone_F_65']
    df['Pct_Senior_Living_Alone'] = alone / df['Senior_Pop'] * 100
    # Grandparents
    # Pct of Householders who are grandparents resp for grandchildren
    # We don't have total householders easily here, assume per 1000 seniors approx proxy
    # Or just use raw count normalized by senior pop
    df['Pct_Grandparent_Carergivers_Pop'] = df['Grandparent_Resp_For_Kids'] / df['Senior_Pop'] * 100
    
    # 7. Disability
    dis = df[['Dis_M_65_74','Dis_M_75+','Dis_F_65_74','Dis_F_75+']].sum(axis=1)
    df['Pct_Senior_Disability'] = dis / df['Senior_Pop'] * 100
    
    # 8. Migration (Moved Last Year)
    # Total Sen - Same House Sen = Movers
    sen_mob_tot = df['Mob_65_74'] + df['Mob_75+']
    sen_same = df['Mob_Same_House_65_74'] + df['Mob_Same_House_75+']
    df['Pct_Senior_Moved_Year'] = (sen_mob_tot - sen_same) / sen_mob_tot * 100
    
    # 9. Veterans
    vets = df[['Vet_M_65_74','Vet_M_75+','Vet_F_65_74','Vet_F_75+']].sum(axis=1)
    df['Pct_Senior_Veterans'] = vets / df['Senior_Pop'] * 100
    
    # 10. Employment
    emp = df[['Emp_M_65_69','Emp_M_70_74','Emp_M_75+','Emp_F_65_69','Emp_F_70_74','Emp_F_75+']].sum(axis=1)
    df['Pct_Senior_Employed'] = emp / df['Senior_Pop'] * 100
    
    # 11. Housing/Economics
    df['Rent_Burden_30+'] = (df['Rent_30_35'] + df['Rent_35_39'] + df['Rent_40_49'] + df['Rent_50+']) / df['Renters_Total'] * 100
    # Median Values direct
    
    return df

def main():
    user_df = load_user_data()
    print("Fetching 100+ census variables (approx 25 derived metrics)...")
    
    census_raw = fetch_census_data(user_df['Zip Code'].tolist())
    if census_raw.empty:
        print("Failed.")
        return
        
    df = process_data(census_raw)
    merged = pd.merge(user_df, df, on='Zip Code', how='left')
    
    # --- OUTPUT ---
    cols_to_corr = [
        'Share',
        'Pct_Population_65+',
        'Med_Inc_Senior_HH',
        'Pct_Senior_Poverty',
        'Pct_Senior_Bach+',
        'Pct_Senior_Broadband',
        'Pct_Senior_No_Comp',
        'Pct_Senior_Living_Alone',
        'Pct_Grandparent_Carergivers_Pop',
        'Pct_Senior_Disability',
        'Pct_Senior_Moved_Year',
        'Pct_Senior_Veterans',
        'Pct_Senior_Employed',
        'Rent_Burden_30+',
        'Med_Home_Value',
        'Med_Gross_Rent',
        'Med_Year_Built',
        'Pct_White_NonHisp',
        'Pct_Black',
        'Pct_Asian',
        'Pct_Hispanic'
    ]
    
    print("\n" + "="*80)
    print("COMPREHENSIVE DEMOGRAPHIC CORRELATIONS (Target: Chat Share)")
    print("="*80)
    
    corr = merged[cols_to_corr].corr()['Share'].sort_values(ascending=False)
    print(corr.round(4))
    
    merged.to_csv('comprehensive_demographic_analysis.csv', index=False)
    print("\nFull dataset saved to 'comprehensive_demographic_analysis.csv'")

if __name__ == "__main__":
    main()
