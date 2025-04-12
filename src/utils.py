from datasketch import MinHash
import pandas as pd
import os
import operator
import json

# Load CSV file into a DataFrame.
def load_csv_table(path: str):
    # Load CSV file and return DataFrame.
    return pd.read_csv(path)

# Compute and return the set of columns that exist in the joined table but not in the base table.
def compute_added_attributes(base_table, joined_table):
    # Return columns in joined_table not in base_table.
    return set(joined_table.columns) - set(base_table.columns)

# Compute the MinHash-based Jaccard similarity between two collections.
def compute_jaccard_similarity(series1, series2, num_perm=128):
    # Compute MinHash-based Jaccard similarity between two collections.
    m1 = MinHash(num_perm=num_perm)
    m2 = MinHash(num_perm=num_perm)
    
    for value in list(series1):
        m1.update(str(value).encode('utf8'))
    for value in list(series2):
        m2.update(str(value).encode('utf8'))
    
    jaccard_sim = m1.jaccard(m2)
    return jaccard_sim

"""
Compute the score for each candidate based solely on its schema.
Score = (# of added attributes present in candidate's schema) / (# of added attributes).
"""
def compute_candidate_schema_scores(added_attrs, candidates, joined_table=None):
    candidates_scores = {}
    for filename, df in candidates.items():
        if "Series_Title" not in df.columns:
            sim = 0.0
        else:
            sim = compute_jaccard_similarity(added_attrs, df.columns)
        if sim > 0.0:
            candidates_scores[filename] = sim
    return candidates_scores

"""
Compute a value-based similarity score for each candidate.
For each added attribute, find the maximum similarity between the joined and candidate column values.
The score is normalized by the ratio of added attributes to candidate columns.
"""
def compute_candidate_values_scores(schema_scores, added_attrs, joined_table_path):
    candidates_scores = {}
    joined_table = load_csv_table(joined_table_path)
    for filename in schema_scores.keys():
        candidate_table = load_csv_table(f"dataset/base_tables_v2/{filename}")
        candidate_table = candidate_table.drop("Series_Title", axis=1)
        candidates_scores[filename] = 0
        for attr in added_attrs:
            max_sim = 0
            for column in candidate_table.columns:
                sim = compute_jaccard_similarity(joined_table[attr], candidate_table[column])
                if sim > max_sim:
                    max_sim = sim
            candidates_scores[filename] += max_sim
        candidates_scores[filename] = candidates_scores[filename] / (len(added_attrs) / len(candidate_table.columns))
    return candidates_scores

# Compare two DataFrames (ignoring their index) and return True if they are identical.
def tables_are_identical(df1, df2):
    try:
        return df1.reset_index(drop=True).equals(df2.reset_index(drop=True))
    except Exception:
        return False

# Load all candidate CSV files from the given directory and return as a dictionary.
def load_candidate_tables(candidate_dir="dataset/base_tables_v2"):
    candidate_files = [f for f in os.listdir(candidate_dir) if f.endswith(".csv")]
    candidates = {}
    for file in candidate_files:
        path = os.path.join(candidate_dir, file)
        candidates[file] = pd.read_csv(path)
    return candidates, candidate_dir

"""
Aggregate schema (40%) and value (60%) scores for each candidate.
Return the best candidate and a dictionary of final scores.
"""
def select_best_candidate(candidates_scores_by_schema, candidates_scores_by_values):
    final_scores = {}
    for table in candidates_scores_by_schema.keys():
        final_scores[table] = (candidates_scores_by_schema[table] * 0.40) + (candidates_scores_by_values[table] * 0.60)
    final_scores = dict(sorted(final_scores.items(), key=lambda item: item[1], reverse=True))
    best_table, best_score = max(final_scores.items(), key=operator.itemgetter(1))
    return (best_table, best_score), final_scores

"""
Test both left and right joins between the source and candidate tables.
Return a dictionary of join results with an 'identical' flag for each join type.
"""
def test_join_types(source_table, candidate_df, joined_table, join_key="Series_Title"):
    join_types = ["left", "right"]
    join_results = {}
    for jt in join_types:
        joined_df = pd.merge(source_table, candidate_df, on=join_key, how=jt)
        identical = tables_are_identical(joined_df, joined_table)
        join_results[jt] = {"dataframe": joined_df, "identical": identical}
        print(f"join type '{jt}': {identical}")
    return join_results

# Return the join type and DataFrame that exactly matches the joined table.
def find_matching_join(join_results):
    for jt, result in join_results.items():
        if result["identical"]:
            return jt, result["dataframe"]
    return None, None

"""
Create an explanation dictionary detailing the transformation:
includes source/joined table names, selected candidate, join type, dropped columns, and the final score.
"""
def create_explanation(source_table_path, joined_table_path, best_candidate,
                        candidate_score, matching_join, candidate_df, joined_df):
    candidate_cols = set(candidate_df.columns)
    joined_cols = set(joined_df.columns)
    dropped_columns = list(candidate_cols - joined_cols)
    explanation = {
        "source_table": os.path.basename(source_table_path),
        "joined_table": os.path.basename(joined_table_path),
        "candidate_used": best_candidate,
        "join_type": matching_join,
        "dropped_columns": dropped_columns,
        "aggregated_similarity_candidate_joined": candidate_score,
        "steps_explanation": (
            "The transformation is obtained by joining the source table with the candidate table '" 
            + best_candidate + "' using the join key 'Series_Title'. The join type '" 
            + matching_join + "' produced an identical result to the joined table. " +
            "The following columns from the candidate table were excluded during the process: " +
            str(dropped_columns)
        )
    }
    return explanation

# Save the explanation as a JSON file in the specified directory.
def save_json_output(explanation, results_dir="results", file_name="reconstructed_transformation.json"):
    if not os.path.exists(results_dir):
        os.makedirs(results_dir, exist_ok=True)
    output_file = os.path.join(results_dir, file_name)
    with open(output_file, "w", encoding="utf8") as f:
        json.dump(explanation, f, indent=4, ensure_ascii=False)
    print("\nReconstructed transformation explanation (JSON):")
    print(json.dumps(explanation, indent=4, ensure_ascii=False))
