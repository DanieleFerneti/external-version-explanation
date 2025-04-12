from datasketch import MinHash

def compute_jaccard_similarity(series1, series2, num_perm=128):

    m1 = MinHash(num_perm=num_perm)
    m2 = MinHash(num_perm=num_perm)
    
    for value in list(series1):
        m1.update(value.encode('utf8'))
    for value in list(series2):
        m2.update(value.encode('utf8'))
    
    jaccard_sim = m1.jaccard(m2)
    return jaccard_sim