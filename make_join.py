import pandas as pd
import random

SEED = 42
random.seed(SEED)

PATH = 'dataset/base_tables/'
base_df = pd.read_csv("dataset/IMDB_Ver_0.csv")
base_len = len(base_df)


ground_truth = pd.DataFrame(columns=[
    'table_name',
    'base_table',
    'joined_tables',
    'join_type',
])

