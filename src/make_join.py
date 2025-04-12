import pandas as pd
import random
import os

SEED = 42
random.seed(SEED)

PATH = 'dataset/base_tables_v2/'
OUTPUT_PATH = 'dataset/joined_tables_v2/'
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Load the base dataset.
base_df = pd.read_csv("dataset/IMDB_Ver_0.csv")
base_len = len(base_df)

# Initialize DataFrame to store join metadata.
ground_truth = pd.DataFrame(columns=[
    'table_name',
    'len',
    'base_table',
    'joined_tables',
    'join_type',
])

i = 0
for filename in os.listdir(PATH):
    file_path = os.path.join(PATH, filename)
    
    # Process only CSV files.
    if os.path.isfile(file_path) and filename.endswith('.csv'):
        try:
            join_candidate = pd.read_csv(file_path)  # Load candidate table.
            
            # Perform left join.
            join_output = pd.merge(base_df, join_candidate, on='Series_Title', how='left')
            i += 1
            join_output.to_csv(f"{OUTPUT_PATH}table_{i}.csv", index=False)
            len_left = len(join_output)
            
            # Perform right join.
            join_output = pd.merge(base_df, join_candidate, on='Series_Title', how='right')
            i += 1
            join_output.to_csv(f"{OUTPUT_PATH}table_{i}.csv", index=False)
            len_right = len(join_output)
            
            # Record metadata for both join types.
            new_row = pd.DataFrame([
                {'table_name': f'table_{i-1}', 'len': f'{len_left}', 'base_table': 'IMDB_Ver_0', 'joined_tables': f'{filename}', 'join_type': 'left'},
                {'table_name': f'table_{i}', 'len': f'{len_right}', 'base_table': 'IMDB_Ver_0', 'joined_tables': f'{filename}', 'join_type': 'right'}
            ])
            ground_truth = pd.concat([ground_truth, new_row], ignore_index=True)
        except KeyError:
            continue

# Save the summary CSV with join operation metadata.
ground_truth.to_csv(f"{OUTPUT_PATH}summary.csv", index=False)
