# Step 1: Make sure these columns exist (rename if your column names are different)
df['United_Enrollment'] = pd.to_numeric(df['United_Enrollment'], errors='coerce').fillna(0)
df['Total_MA_Enrollment'] = pd.to_numeric(df['Total_MA_Enrollment'], errors='coerce').fillna(1)
df['Total_Pop_65_Plus'] = pd.to_numeric(df['Total_Pop_65_Plus'], errors='coerce').fillna(1)

# Step 2: Calculate United gap (lower United % = higher score)
df['United_Penetration'] = df['United_Enrollment'] / df['Total_MA_Enrollment']
df['United_Gap_Score'] = 1 / (df['United_Penetration'] + 0.05)   # +0.05 to avoid division by zero

# Step 3: Calculate overall MA saturation (lower saturation = more room to grow)
df['MA_Penetration'] = df['Total_MA_Enrollment'] / df['Total_Pop_65_Plus']
df['Market_Room_Score'] = 1 - df['MA_Penetration']   # the lower the current MA %, the higher the score

# Step 4: Final Blue Sea Score = 60% Digital Readiness + 40% Opportunity
df['Blue_Sea_Score'] = (
    df['Digital_Readiness'] * 0.6 +           # Your original excellent digital score
    df['United_Gap_Score'] * 4 +              # United gap is KING — heavy weight
    df['Market_Room_Score'] * 2               # Extra points if overall MA is still low
)

# Step 5: Final ranking
df['Blue_Sea_Rank'] = df['Blue_Sea_Score'].rank(ascending=False)

# Step 6: Show top 20 (you will finally see Lake, Broward, Sumter at the top)
top20 = df[['County', 'State', 'Digital_Readiness', 'United_Penetration', 'MA_Penetration', 'Blue_Sea_Score', 'Blue_Sea_Rank']].sort_values('Blue_Sea_Rank').head(20)
print(top20)

# Optional: Save the final list
top20.to_csv('FINAL_BLUE_SEA_TOP20.csv', index=False)
