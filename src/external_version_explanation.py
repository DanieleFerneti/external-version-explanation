import os
import json
import pandas as pd
from datasketch import MinHash

def compute_similarity_multiple(series1, series2, num_perm=128):
    """
    Calcola la similarità tra due serie utilizzando MinHash (Jaccard)
    e il containment.
    (Questa funzione non viene più usata per il calcolo dei punteggi a livello di schema.)
    """
    m1 = MinHash(num_perm=num_perm)
    m2 = MinHash(num_perm=num_perm)
    for value in series1.astype(str).tolist():
        m1.update(value.encode('utf8'))
    for value in series2.astype(str).tolist():
        m2.update(value.encode('utf8'))
    jaccard_sim = m1.jaccard(m2)
    
    set1 = set(series1.astype(str).tolist())
    set2 = set(series2.astype(str).tolist())
    containment_sim = len(set1.intersection(set2)) / len(set1) if set1 else 0.0
    
    return {"jaccard": jaccard_sim, "containment": containment_sim}

def tables_are_identical(df1, df2):
    try:
        return df1.reset_index(drop=True).equals(df2.reset_index(drop=True))
    except Exception:
        return False

def load_base_table(path="dataset/IMDB_Ver_0.csv"):
    base_table = pd.read_csv(path)
    #print("Base Table caricata da:", path)
    return base_table, path

def load_joined_table(joined_dir="dataset/joined_tables_v2"):
    # Per semplicità si fissa il nome della Joined Table (es. "table_1.csv")
    joined_filename = "table_8.csv"
    joined_path = os.path.join(joined_dir, joined_filename)
    joined_table = pd.read_csv(joined_path)
    print("\nJoined Table caricata da: " + joined_filename + "\n")
    return joined_table, joined_path

def compute_added_attributes(base_table, joined_table):
    # Calcola le colonne aggiunte: cioè quelle presenti nella Joined Table ma non nella Base Table.
    added_attrs = set(joined_table.columns) - set(base_table.columns)
    #print("Attributi aggiunti nella Joined Table:", list(added_attrs))
    return added_attrs

def load_candidate_tables(candidate_dir="dataset/base_tables_v2"):
    candidate_files = [f for f in os.listdir(candidate_dir) if f.endswith(".csv")]
    candidates = {}
    for file in candidate_files:
        path = os.path.join(candidate_dir, file)
        candidates[file] = pd.read_csv(path)
    return candidates, candidate_dir

def compute_candidate_scores(added_attrs, candidates, joined_table=None):
    """
    Calcola il punteggio per ciascun candidato basandosi esclusivamente sullo schema:
    il punteggio viene calcolato come il rapporto:
        (numero di added attributes presenti nello schema del candidato) / (numero totale di added attributes)
    """
    candidate_scores = {}
    candidate_common_attrs = {}
    for filename, df in candidates.items():
        # Considera solo i candidati che contengono la chiave di join "Series_Title"
        if "Series_Title" not in df.columns:
            print(f"Salto il candidato {filename} perché non contiene la colonna 'Series_Title'")
            continue
        # common_attrs sono gli attributi aggiunti (definiti dallo schema della Joined Table)
        # che il candidato possiede
        common_attrs = added_attrs.intersection(set(df.columns))
        if not common_attrs:
            continue
        # Calcola il punteggio: frazione degli attributi aggiunti presenti
        aggregated_sim = len(common_attrs) / len(added_attrs)  
        candidate_scores[filename] = aggregated_sim
        candidate_common_attrs[filename] = list(common_attrs)
        print(f"Candidate {filename} - Similarità aggregata: {aggregated_sim:.2f}\n")
    return candidate_scores, candidate_common_attrs

def select_best_candidate(candidate_scores, candidate_common_attrs):
    if not candidate_scores:
        raise ValueError("Nessuna tabella candidata contiene attributi aggiunti della Joined Table.")
    best_candidate = max(candidate_scores, key=candidate_scores.get)
    print("\nLa tabella candidata migliore è:", best_candidate)
    print("\nPunteggio aggregato:", candidate_scores[best_candidate])
    return best_candidate

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

def main_flow():
    # Step 1: Load Base Table
    base_table, base_table_path = load_base_table()
    
    # Step 2: Load Joined Table
    joined_table, joined_table_path = load_joined_table()
    
    # Step 3: Compute Added Attributes (solo quelle aggiunte allo schema, cioè le colonne nuove della Joined Table)
    added_attrs = compute_added_attributes(base_table, joined_table)
    
    # Step 4: Load Candidate Tables
    candidates, candidate_dir = load_candidate_tables()
    
    # Step 5: Compute Candidate Scores utilizzando solo i nomi (lo schema) delle colonne aggiunte
    candidate_scores, candidate_common_attrs = compute_candidate_scores(added_attrs, candidates)
    
    # Step 6: Select Best Candidate
    best_candidate = select_best_candidate(candidate_scores, candidate_common_attrs)
    best_candidate_path = os.path.join(candidate_dir, best_candidate)
    candidate_df = pd.read_csv(best_candidate_path)
    
    # Step 7: Test Join Types
    join_results = test_join_types(base_table, candidate_df, joined_table)
    
    # Step 8: Find Matching Join
    matching_join, joined_df = find_matching_join(join_results)
    
    if matching_join:
        print("\nIl join type che produce una tabella identica è:", matching_join)
        # Step 9: Create Explanation
        explanation = create_explanation(base_table_path, joined_table_path,
                                         best_candidate, candidate_common_attrs,
                                         candidate_scores[best_candidate], matching_join,
                                         candidate_df, joined_df)
        # Step 10: Save JSON Output
        save_json_output(explanation)
    else:
        print("\nNessun tipo di join produce una tabella identica alla Joined Table. " +
              "Verifica se la trasformazione include altre operazioni o se il candidato selezionato non è quello corretto.")

if __name__ == "__main__":
    main_flow()
