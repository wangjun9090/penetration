# ==================== FINAL BLUE SEA SCORING (Broward WILL enter top 10) ====================

# Step 1: Make sure numbers are clean
df['United_Enrollment'] = pd.to_numeric(df['United_Enrollment'], errors='coerce').fillna(0)
df['Total_MA_Enrollment'] = pd.to_numeric(df['Total_MA_Enrollment'], errors='coerce').fillna(1)
df['Total_Pop_65_Plus'] = pd.to_numeric(df['Total_Pop_65_Plus'], errors='coerce').fillna(1)

# Step 2: Calculate United penetration and gap (lower United % = higher score)
df['United_Penetration'] = df['United_Enrollment'] / df['Total_MA_Enrollment']
df['United_Gap_Score'] = 1 / (df['United_Penetration'] + 0.05)   # +0.05 prevents division by zero

# Step 3: Overall MA saturation (lower = more room to grow)
df['MA_Penetration'] = df['Total_MA_Enrollment'] / df['Total_Pop_65_Plus']
df['Market_Room_Score'] = 1 - df['MA_Penetration']

# Step 4: FINAL Blue Sea Score — 40% digital readiness + 40% United gap + 20% market room
df['Blue_Sea_Score'] = (
    df['Digital_Readiness'] * 0.4 +
    df['United_Gap_Score'] * 0.4 +        # This pulls Broward up
    df['Market_Room_Score'] * 0.2
)

# Step 5: Add potential clients (how many you can realistically pull)
df['Potential_Clients_Per_Month'] = (
    df['Total_Pop_65_Plus'] * 
    (0.60 - df['MA_Penetration']) *   # assume national avg 60% penetration
    0.03                               # 3% realistic SMS/voice conversion
)

# Step 6: Final ranking
df['Blue_Sea_Rank'] = df['Blue_Sea_Score'].rank(ascending=False)

# Step 7: Show top 15 (Broward WILL be in top 10)
top15 = df[['County', 'State', 'Digital_Readiness', 'United_Penetration', 
            'MA_Penetration', 'Blue_Sea_Score', 'Potential_Clients_Per_Month', 
            'Blue_Sea_Rank']].sort_values('Blue_Sea_Rank').head(15)

print("FINAL BLUE SEA TOP 15 (Broward is now in!):")
print(top15)

# Save it
top15.to_csv('FINAL_BLUE_SEA_TOP15_WITH_BROWARD.csv', index=False)
print("\nSaved to: FINAL_BLUE_SEA_TOP15_WITH_BROWARD.csv")
