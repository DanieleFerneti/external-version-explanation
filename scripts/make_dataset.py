import pandas as pd
import random
import string
import os

SEED = 42
random.seed(SEED)

# Funzione per generare dati casuali
def random_string(length=15):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_float(low=1.0, high=30.0):
    return round(random.uniform(low, high), 2)

def random_int(low=1, high=300):
    return random.randint(low, high)


PATH = 'dataset/base_tables_v2/'
os.makedirs(PATH, exist_ok=True)


base_df = pd.read_csv("dataset/IMDB_Ver_0.csv")
base_len = len(base_df)

### 1. Viewer_Engagement_Metrics
Viewer_Engagement_Metrics = pd.DataFrame({
    'Series_Title': base_df['Series_Title'],
    'Viewer_Retention': [random_float(50, 100) for _ in range(base_len)],
    'Soundtrack_Score': [random_float(1, 10) for _ in range(base_len)],
    'Scene_Count': [random_int(50, 150) for _ in range(base_len)],
    'Episode_Length_Avg': [random_int(20, 60) for _ in range(base_len)],
    'Cinematography_Style': [random_string() for _ in range(base_len)]
})
Viewer_Engagement_Metrics.to_csv(f"{PATH}Viewer_Engagement_Metrics.csv", index=False)
print("dataset/Viewer_Engagement_Metrics.csv creato")

### 2. Production_Quality_Stats
Production_Quality_Stats = pd.DataFrame({
    'Series_Title': base_df['Series_Title'],
    'Budget_Category': [random.choice(["Low", "Medium", "High"]) for _ in range(base_len)],
    'Visual_Effects_Quality': [random.choice(["Poor", "Average", "Excellent"]) for _ in range(base_len)],
    'Dialogue_Density': [random_float(0.1, 1.0) for _ in range(base_len)],
    'Season_Count': [random_int(1, 10) for _ in range(base_len)],
    'Originality_Score': [random_float(1, 10) for _ in range(base_len)]
})
Production_Quality_Stats.to_csv(f"{PATH}Production_Quality_Stats.csv", index=False)
print("dataset/Production_Quality_Stats.csv creato")

### 3. Artistic_And_Merch_Data
k = 300
Artistic_And_Merch_Data = pd.DataFrame({
    'Series_Title': random.sample(list(base_df['Series_Title']), k=k),
    'Set_Design_Type': [random.choice(["Futuristic", "Historical", "Contemporary"]) for _ in range(k)],
    'Cultural_Impact_Score': [random_float(1, 10) for _ in range(k)],
    'Merchandise_Sales': [random_int(1000, 1000000) for _ in range(k)],
    'Rewatchability_Index': [random_float(50, 90) for _ in range(k)],
    'Opening_Credits_Length': [random_int(30, 120) for _ in range(k)]
})
Artistic_And_Merch_Data.to_csv(f"{PATH}Artistic_And_Merch_Data.csv", index=False)
print("dataset/Artistic_And_Merch_Data.csv creato")

### 4. Director_And_Fan_Reception
Director_And_Fan_Reception = pd.DataFrame({
    'Series_Title': base_df['Series_Title'],
    'Director_Experience_Years': [random_int(5, 40) for _ in range(base_len)],
    'Streaming_Rank': [random_int(1, 100) for _ in range(base_len)],
    'Fan_Engagement_Level': [random.choice(["Low", "Medium", "High"]) for _ in range(base_len)],
    'Episodes_With_Cliffhanger': [random_int(0, 10) for _ in range(base_len)],
    'Critical_Consensus': [random.choice(["Positive", "Neutral", "Negative"]) for _ in range(base_len)]
})
Director_And_Fan_Reception.to_csv(f"{PATH}Director_And_Fan_Reception.csv", index=False)
print("dataset/Director_And_Fan_Reception.csv creato")

### 5. Behind_The_Scenes_Info
k=470
Behind_The_Scenes_Info = pd.DataFrame({
    'Series_Title': random.sample(list(base_df['Series_Title']), k=k),
    'Behind_The_Scenes_Hours': [random_int(10, 500) for _ in range(k)],
    'Spin_Offs_Count': [random_int(0, 5) for _ in range(k)],
    'Script_Revisions': [random_int(1, 20) for _ in range(k)],
    'Premiere_Country': [random.choice(["USA", "UK", "Canada"]) for _ in range(k)],
    'Viral_Moments_Count': [random_int(0, 50) for _ in range(k)]
})
Behind_The_Scenes_Info.to_csv(f"{PATH}Behind_The_Scenes_Info.csv", index=False)
print("dataset/Behind_The_Scenes_Info.csv creato")

### 6. Casting_Info (con N1 e N2: Production_Company, Lead_Actor)
Casting_Info = pd.DataFrame({
    'Series_Title': base_df['Series_Title'],
    'Production_Company': [random_string() for _ in range(base_len)],
    'Lead_Actor': [random_string() for _ in range(base_len)]
})
Casting_Info.to_csv(f"{PATH}Casting_Info.csv", index=False)
print("dataset/Casting_Info.csv creato")

### 7. Casting_And_Filming_Details (include N1, N2 + uno nuovo)
Casting_And_Filming_Details = pd.DataFrame({
    'Series_Title': base_df['Series_Title'],
    'Production_Company': Casting_Info['Production_Company'],
    'Lead_Actor': Casting_Info['Lead_Actor'],
    'Filming_Duration': [random.choice(["3 months", "6 months", "1 year"]) for _ in range(base_len)]
})
Casting_And_Filming_Details.to_csv(f"{PATH}Casting_And_Filming_Details.csv", index=False)
print("dataset/Casting_And_Filming_Details.csv creato")

### 8. Production_And_Creatives (altri attributi inventati + Series_Title)
Production_And_Creatives = pd.DataFrame({
    'Series_Title': base_df['Series_Title'],
    'Production_Company': Casting_Info['Production_Company'],
    'Costume_Designer': [random_string() for _ in range(base_len)],
    'Script_Language': [random.choice(["English", "Spanish", "French"]) for _ in range(base_len)],
    'Original_Sound_Composer': [random_string() for _ in range(base_len)]
})
Production_And_Creatives.to_csv(f"{PATH}Production_And_Creatives.csv", index=False)
print("dataset/Production_And_Creatives.csv creato")

### 9. Distribution_And_Awards (senza Series_Title)
Distribution_And_Awards = pd.DataFrame({
    'Release_Format': [random.choice(["Streaming", "Theater", "DVD"]) for _ in range(base_len)],
    'Production_Company': Casting_Info['Production_Company'],
    'Lead_Actor': Casting_Info['Lead_Actor'],
    'Award_Nominations': [random_int(0, 20) for _ in range(base_len)],
    'Studio_Location': [random.choice(["Los Angeles", "London", "Paris"]) for _ in range(base_len)]
})
Distribution_And_Awards.to_csv(f"{PATH}Distribution_And_Awards.csv", index=False)
print("dataset/Distribution_And_Awards.csv creato")

### 10. Core_Crew_Info (solo N1, N2)
Core_Crew_Info = pd.DataFrame({
    'Production_Company': Casting_Info['Production_Company'],
    'Lead_Actor': Casting_Info['Lead_Actor']
})
Core_Crew_Info.to_csv(f"{PATH}Core_Crew_Info.csv", index=False)
print("dataset/Core_Crew_Info.csv creato")
