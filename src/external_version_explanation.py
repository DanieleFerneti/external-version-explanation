import os
import json
import pandas as pd
from datasketch import MinHash
import utils

JOINED_TABLE_PATH = "dataset/joined_tables_v2"
BASE_TABLE_PATH = "dataset/IMDB_Ver_0.csv"
JOINED_TABLE_NAME = "table_4.csv"

if __name__ == "__main__":

    # Step 1: Load Base Table
    BASE_TABLE = utils.load_csv_table(BASE_TABLE_PATH)
    print(f"-- BASE_TABLE: {BASE_TABLE_PATH}")
    
    # Step 2: Load Joined Table
    JOINED_TABLE = utils.load_csv_table(os.path.join(JOINED_TABLE_PATH, JOINED_TABLE_NAME))
    print(f"-- JOINED_TABLE: {os.path.join(JOINED_TABLE_PATH, JOINED_TABLE_NAME)}")

    # Step 3: Compute Added Attributes (solo quelle aggiunte allo schema, cioè le colonne nuove della Joined Table)
    added_attrs = utils.compute_added_attributes(BASE_TABLE, JOINED_TABLE)
    print(f"-- Added attributes found: {added_attrs}")
    
    # Step 4: Load Candidate Tables
    candidates, candidate_dir = utils.load_candidate_tables()
    
    # Step 5: Compute Candidate Scores based on schema and attributes values
    candidates_scores_by_schema = utils.compute_candidate_schema_scores(added_attrs, candidates)
    candidates_scores_by_schema = dict(sorted(candidates_scores_by_schema.items(), key=lambda item: item[1], reverse=True))
    candidates_scores_by_values = utils.compute_candidate_values_scores(candidates_scores_by_schema, added_attrs, os.path.join(JOINED_TABLE_PATH, JOINED_TABLE_NAME))
    
    # Step 6: Select Best Candidate
    best_candidate = utils.select_best_candidate(candidates_scores_by_schema, candidates_scores_by_values)
    print('-- Best Candidate tables')
    for candidate in best_candidate.keys():
        print(f'** "{candidate}": {best_candidate[candidate]}')

    # best_candidate_path = os.path.join(candidate_dir, best_candidate)
    # candidate_df = pd.read_csv(best_candidate_path)
    
    # # Step 7: Test Join Types
    # join_results = test_join_types(BASE_TABLE, candidate_df, JOINED_TABLE)
    
    # # Step 8: Find Matching Join
    # matching_join, joined_df = find_matching_join(join_results)
    
    # if matching_join:
    #     print("\nIl join type che produce una tabella identica è:", matching_join)
    #     # Step 9: Create Explanation
    #     explanation = create_explanation(base_table_path, joined_table_path,
    #                                      best_candidate, candidate_common_attrs,
    #                                      candidate_scores[best_candidate], matching_join,
    #                                      candidate_df, joined_df)
    #     # Step 10: Save JSON Output
    #     save_json_output(explanation)
    # else:
    #     print("\nNessun tipo di join produce una tabella identica alla Joined Table. " +
    #           "Verifica se la trasformazione include altre operazioni o se il candidato selezionato non è quello corretto.")


