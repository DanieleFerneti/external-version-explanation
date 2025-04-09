import os
import json
import pandas as pd
from datasketch import MinHash


# ===============================
# Parte 1: Generazione delle New Tables e del Ground Truth
# ===============================

# 1. Creazione della Base Table (con N colonne)
data_original = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
    'value': [10, 20, 30, 40, 50]
}
df_base = pd.DataFrame(data_original)
print("Base Table:")
print(df_base)
print("\n")

# 2. Creazione delle Synthetic (Candidate) Tables
# Candidate 1: contiene 'id' e 'date' -> utile per spiegare l'aggiunta della colonna 'date'
data_candidate1 = {
    'id': [2, 3, 4, 6],
    'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
}
df_candidate1 = pd.DataFrame(data_candidate1)

# Candidate 2: contiene 'id' e 'score' -> non utile se il nuovo attributo da spiegare è 'date'
data_candidate2 = {
    'id': [1, 2, 5, 7],
    'score': [95, 85, 75, 65]
}
df_candidate2 = pd.DataFrame(data_candidate2)

# Salva le candidate in un dizionario per uso successivo
synthetic_tables = {
    "Candidate1": df_candidate1,
    "Candidate2": df_candidate2
}

# Specifica del nuovo attributo da spiegare
new_attribute = 'date'

# 3. Esecuzione dei join per creare le New Tables
join_types = ["inner", "left", "outer"]

# Dizionari per salvare i risultati e il ground truth
new_tables = {}
ground_truth = {}

for candidate_name, df_candidate in synthetic_tables.items():
    if new_attribute not in df_candidate.columns:
        continue  # Salta la candidate se non ha il campo richiesto
    for jt in join_types:
        joined_table = pd.merge(df_base, df_candidate, on='id', how=jt)
        # Calcolo del data loss in percentuale:
        if jt == "inner":
            data_loss = 100 * (1 - len(joined_table) / len(df_base))
        elif jt == "left":
            data_loss = 100 * (1 - joined_table[new_attribute].notnull().sum() / len(joined_table))
        else:  # outer
            base_ids = set(df_base['id'])
            joined_ids = set(joined_table['id'])
            missing_ratio = 100 * (1 - len(base_ids.intersection(joined_ids)) / len(base_ids))
            data_loss = missing_ratio

        # Determina quali colonne della candidate non compaiono nel risultato (simula un'operazione di proiezione)
        candidate_cols = set(df_candidate.columns)
        new_table_cols = set(joined_table.columns)
        dropped_columns = list(candidate_cols - new_table_cols)

        # Crea una chiave unica per identificare la new table generata
        key = f"{candidate_name}_{jt}"
        new_tables[key] = joined_table

        ground_truth[key] = {
            "candidate_used": candidate_name,
            "join_type": jt,
            "dropped_columns": dropped_columns,
            "data_loss_percent": round(data_loss, 2)
        }

# Salva il file ground truth in formato JSON nella cartella results
parent_dir = os.path.dirname(os.getcwd())
results_dir = os.path.join(parent_dir, "results")
if not os.path.exists(results_dir):
    os.makedirs(results_dir)

ground_truth_file = os.path.join(results_dir, "ground_truth.json")
with open(ground_truth_file, "w") as f:
    json.dump(ground_truth, f, indent=4)
print(f"Ground truth salvato in: {ground_truth_file}\n")

print("New tables generate:")
for key in new_tables:
    print(key)
print("\n")


# ===============================
# Parte 2: Reverse Engineering della Trasformazione
# ===============================

# Funzione di similarità che utilizza MinHash (Jaccard), normalized Levenshtein e il containment
def compute_similarity_multiple(series1, series2, num_perm=128):
    # Similarità Jaccard tramite MinHash (datasketch)
    m1 = MinHash(num_perm=num_perm)
    m2 = MinHash(num_perm=num_perm)
    for value in series1.astype(str).tolist():
        m1.update(value.encode('utf8'))
    for value in series2.astype(str).tolist():
        m2.update(value.encode('utf8'))
    jaccard_sim = m1.jaccard(m2)
    
    
    # Similarità di Containment:
    # Assumiamo di considerare la new table come riferimento (Q) e la candidate come X:
    set_target = set(series1.astype(str).tolist())
    set_candidate = set(series2.astype(str).tolist())
    containment_sim = len(set_target.intersection(set_candidate)) / len(set_target) if set_target else 0.0
    
    return {"jaccard": jaccard_sim, "containment": containment_sim}

# Funzione per confrontare due DataFrame
def tables_are_similar(df1, df2):
    try:
        return df1.reset_index(drop=True).equals(df2.reset_index(drop=True))
    except Exception as e:
        return False

# Funzione per ricostruire la trasformazione
def reconstruct_transformation(base_table, new_table, synthetic_tables, join_types=["inner", "left", "outer"]):
    # 1. Identifica le colonne aggiunte nella new_table rispetto alla base_table
    added_attributes = list(set(new_table.columns) - set(base_table.columns))
    if not added_attributes:
        print("Non sono state aggiunte colonne rispetto alla Base Table.")
        return None, None
    print("Attributi aggiunti trovati:", added_attributes)
    
    # Supponiamo di voler spiegare il primo attributo aggiunto, ad es. 'date'
    target_attribute = added_attributes[0]
    
    # 2. Per ogni candidate che contiene il target_attribute, calcola la similarità usando più metodi
    candidate_scores = {}
    for candidate_name, candidate_df in synthetic_tables.items():
        if target_attribute in candidate_df.columns:
            sim_dict = compute_similarity_multiple(candidate_df[target_attribute], new_table[target_attribute])
            best_method_value = max(sim_dict.values())
            candidate_scores[candidate_name] = best_method_value
            print(f"Similarità per candidate {candidate_name} (best method): {best_method_value:.2f} - Dettagli: {sim_dict}")
    
    if not candidate_scores:
        print("Nessuna candidate table contiene il target attribute.")
        return None, None

    best_candidate = max(candidate_scores, key=candidate_scores.get)
    print("Candidate migliore sulla base della similarità:", best_candidate)
    
    # 3. Testa diversi tipi di join con la candidate selezionata
    best_join_type = None
    best_match = None
    best_match_score = -1
    candidate_df = synthetic_tables[best_candidate]
    for jt in join_types:
        candidate_join = pd.merge(base_table, candidate_df, on="id", how=jt)
        sim_dict = compute_similarity_multiple(candidate_join.fillna("NaN").stack(), new_table.fillna("NaN").stack())
        similarity = max(sim_dict.values())
        print(f"Tipo di join '{jt}' produce similarità: {similarity:.2f} - Dettagli: {sim_dict}")
        if similarity > best_match_score:
            best_match_score = similarity
            best_match = candidate_join
            best_join_type = jt

    print("\nIl miglior tipo di join è:", best_join_type)
    if not tables_are_similar(best_match, new_table):
        print("Attenzione: anche il miglior join non produce una tabella identica alla New Table.")
    else:
        print("Il risultato del join corrisponde perfettamente alla New Table.")
    
    explanation = {
        "candidate_used": best_candidate,
        "join_type": best_join_type,
        "target_attribute": target_attribute,
        "similarity_candidate_new": round(candidate_scores[best_candidate], 2),
        "final_join_similarity": round(best_match_score, 2)
    }
    return explanation, best_match

# Seleziona una delle new tables generate per il reverse engineering
selected_new_table_key = "Candidate1_left"  # Ad es., quella prodotta con Candidate1 e join 'left'
df_new = new_tables[selected_new_table_key]
print("\n--- Reverse Engineering per la new table:", selected_new_table_key, "---")
explanation, reconstructed_table = reconstruct_transformation(df_base, df_new, synthetic_tables)
print("\nSpiegazione della trasformazione ricostruita:")
print(json.dumps(explanation, indent=4))

explained_file = os.path.join(results_dir, "reconstructed_transformation.json")
with open(explained_file, "w") as f:
    json.dump(explanation, f, indent=4)
print(f"\nLa spiegazione della trasformazione è stata salvata in: {explained_file}")
