import os
import pandas as pd
from datasketch import MinHash
import utils

JOINED_TABLE_PATH = "dataset/joined_tables_v2"
BASE_TABLE_PATH = "dataset/base_tables_v2"
SOURCE_TABLE_PATH = "dataset/IMDB_Ver_0.csv"
JOINED_TABLE_NAME = "table_5.csv"

if __name__ == "__main__":

    # Step 1: Load Base Table
    SOURCE_TABLE = utils.load_csv_table(SOURCE_TABLE_PATH)
    print(f"-- SOURCE_TABLE: {SOURCE_TABLE_PATH}")
    
    # Step 2: Load Joined Table
    JOINED_TABLE = utils.load_csv_table(os.path.join(JOINED_TABLE_PATH, JOINED_TABLE_NAME))
    print(f"-- JOINED_TABLE: {os.path.join(JOINED_TABLE_PATH, JOINED_TABLE_NAME)}")

    # Step 3: Compute Added Attributes (solo quelle aggiunte allo schema, cioè le colonne nuove della Joined Table)
    added_attrs = utils.compute_added_attributes(SOURCE_TABLE, JOINED_TABLE)
    print(f"-- Added attributes found: {added_attrs}")
    
    # Step 4: Load Candidate Tables
    candidates, candidate_dir = utils.load_candidate_tables()
    
    # Step 5: Compute Candidate Scores based on schema and attributes values
    candidates_scores_by_schema = utils.compute_candidate_schema_scores(added_attrs, candidates)
    candidates_scores_by_schema = dict(sorted(candidates_scores_by_schema.items(), key=lambda item: item[1], reverse=True))
    candidates_scores_by_values = utils.compute_candidate_values_scores(candidates_scores_by_schema, added_attrs, os.path.join(JOINED_TABLE_PATH, JOINED_TABLE_NAME))
    
    # Step 6: Select Best Candidate
    (best_table, best_score), top_candidates = utils.select_best_candidate(candidates_scores_by_schema, candidates_scores_by_values)
    print('-- Top Candidates tables')
    for candidate in top_candidates.keys():
        print(f'** "{candidate}": {top_candidates[candidate]}')

    # Load the best candidate table as a DataFrame from the candidate tables directory.
    best_candidate_path = os.path.join(BASE_TABLE_PATH, best_table)
    best_df = utils.load_csv_table(best_candidate_path)
    
    # Step 7: Test Join Types
    join_results = utils.test_join_types(SOURCE_TABLE, best_df, JOINED_TABLE)
    
    # Step 8: Find Matching Join
    matching_join, joined_df = utils.find_matching_join(join_results)
    
    if matching_join:
        print("\nThe join type that produces an identical table is:", matching_join)
        # Step 9: Create Explanation
        explanation = utils.create_explanation(SOURCE_TABLE_PATH, JOINED_TABLE_NAME,
                                         best_table,
                                         best_score, matching_join,
                                         best_df, joined_df)
        # Step 10: Save JSON Output
        utils.save_json_output(explanation)
    else:
        print("\nNo join type produces an identical table to the Joined Table. " +
              "Check if the transformation includes other operations or if the selected candidate is incorrect.")


