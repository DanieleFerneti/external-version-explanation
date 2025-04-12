from datasketch import MinHash
import pandas as pd
import os


def load_csv_table(path: str):
    return pd.read_csv(path)


def compute_added_attributes(base_table, joined_table):
    # Calcola le colonne aggiunte: cioè quelle presenti nella Joined Table ma non nella Base Table.
    return set(joined_table.columns) - set(base_table.columns)


def compute_jaccard_similarity(series1, series2, num_perm=128):
    m1 = MinHash(num_perm=num_perm)
    m2 = MinHash(num_perm=num_perm)
    
    for value in list(series1):
        m1.update(str(value).encode('utf8'))
    for value in list(series2):
        m2.update(str(value).encode('utf8'))
    
    jaccard_sim = m1.jaccard(m2)
    return jaccard_sim


def compute_candidate_schema_scores(added_attrs, candidates, joined_table=None):
    """
    Calcola il punteggio per ciascun candidato basandosi esclusivamente sullo schema:
    il punteggio viene calcolato come il rapporto:
        (numero di added attributes presenti nello schema del candidato) / (numero totale di added attributes)
    """
    candidates_scores = {}

    for filename, df in candidates.items():
        if "Series_Title" not in df.columns:
            sim = 0.0
        else:
            sim = compute_jaccard_similarity(added_attrs, df.columns)

        if sim > 0.0:
            candidates_scores[filename] = sim

    return candidates_scores


def compute_candidate_values_scores(schema_scores, added_attrs, joined_table_path):
    candidates_scores = {}
    joined_table = load_csv_table(joined_table_path)

    for filename in schema_scores.keys():
        candidate_table = load_csv_table(f"dataset/base_tables_v2/{filename}")
        candidate_table = candidate_table.drop("Series_Title", axis=1)
        candidates_scores[filename] = 0

        for attr in added_attrs:
            max_sim = 0
            sim = 0
            for column in candidate_table.columns:
                sim = compute_jaccard_similarity(joined_table[attr], candidate_table[column])    
                if sim > max_sim:
                    max_sim = sim
            candidates_scores[filename] =  candidates_scores[filename] + max_sim
        candidates_scores[filename] = candidates_scores[filename]/(len(added_attrs)/len(candidate_table.columns))

    return candidates_scores




def tables_are_identical(df1, df2):
    try:
        return df1.reset_index(drop=True).equals(df2.reset_index(drop=True))
    except Exception:
        return False



def load_candidate_tables(candidate_dir="dataset/base_tables_v2"):
    candidate_files = [f for f in os.listdir(candidate_dir) if f.endswith(".csv")]
    candidates = {}
    for file in candidate_files:
        path = os.path.join(candidate_dir, file)
        candidates[file] = pd.read_csv(path)
    return candidates, candidate_dir



def select_best_candidate(candidates_scores_by_schema, candidates_scores_by_values):
    if not candidates_scores_by_schema:
        raise ValueError("Nessuna tabella candidata contiene attributi aggiunti della Joined Table.")

    final_scores = {}

    for table in candidates_scores_by_schema.keys():
        final_scores[table] = (candidates_scores_by_schema[table] * 0.40) + (candidates_scores_by_values[table] * 0.60)

    final_scores = dict(sorted(final_scores.items(), key=lambda item: item[1], reverse=True))
    return final_scores

def test_join_types(base_table, candidate_df, joined_table, join_key="Series_Title"):
    join_types = ["left", "right"]
    join_results = {}
    for jt in join_types:
        joined_df = pd.merge(base_table, candidate_df, on=join_key, how=jt)
        identical = tables_are_identical(joined_df, joined_table)
        join_results[jt] = {"dataframe": joined_df, "identical": identical}
        print(f"\nJoin type '{jt}' produce una tabella identica? {identical}")
    return join_results

def find_matching_join(join_results):
    for jt, result in join_results.items():
        if result["identical"]:
            return jt, result["dataframe"]
    return None, None

def create_explanation(base_table_path, joined_table_path, best_candidate,
                       candidate_common_attrs, candidate_score, matching_join,
                       candidate_df, joined_df):
    candidate_cols = set(candidate_df.columns)
    joined_cols = set(joined_df.columns)
    dropped_columns = list(candidate_cols - joined_cols)
    explanation = {
        "base_table": os.path.basename(base_table_path),
        "joined_table": os.path.basename(joined_table_path),
        "candidate_used": best_candidate,
        "common_attributes_used": candidate_common_attrs[best_candidate],
        "join_type": matching_join,
        "dropped_columns": dropped_columns,
        "aggregated_similarity_candidate_joined": candidate_score,
        "steps_explanation": (
            "La trasformazione è stata ottenuta partendo dalla Base Table, "
            "a cui è stata joinata la tabella candidata '" + best_candidate + "' "
            "utilizzando la colonna 'Series_Title' come chiave di join. "
            "Il join di tipo '" + matching_join + "' ha prodotto esattamente la Joined Table: "
            "durante il processo, le seguenti colonne della tabella candidata sono state escluse: " +
            str(dropped_columns)
        )
    }
    return explanation

def save_json_output(explanation, results_dir="results", file_name="reconstructed_transformation.json"):
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    output_file = os.path.join(results_dir, file_name)
    with open(output_file, "w", encoding="utf8") as f:
        json.dump(explanation, f, indent=4, ensure_ascii=False)
    print("\nSpiegazione della trasformazione ricostruita (JSON):")
    print(json.dumps(explanation, indent=4, ensure_ascii=False))