import snowflake.snowpark as snowpark
from snowflake.cortex import Complete, CompleteOptions, embed_text_1024
from snowflake.snowpark.functions import col, avg, when, length, ln, lit, sum as sf_sum, count, coalesce, sql_expr, udf, call_function
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import BooleanType, StringType
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm
import os
from time import time
import json
import random
from rapidfuzz import fuzz
import numpy as np
from langdetect import detect_langs
import re

MAX_WORKERS = min(64, (os.cpu_count() or 4) * 5)

def load_data(dbt):
    print("------------------------------------------------------------")
    print("Step 1: Loading input data...")
    print("------------------------------------------------------------")

    dfs = {
        'Master': dbt.ref("raw_pos_master").cache_result(),
        'ENR 250': dbt.ref("raw_pos_enr_250").cache_result(),
        'ENR 400': dbt.ref("raw_pos_enr_400").cache_result(),
        'ENR 600': dbt.ref("raw_pos_enr_600").cache_result(),
        'Global data': dbt.ref("raw_pos_global_data").cache_result(),
    }
    if 'Master' not in dfs:
        raise ValueError("Master dataset not found in loaded data.")
    print(f"✔ Data on {len(dfs)} datasets successfully loaded from Snowflake\n")
    return dfs

def generate_column_pairs(dfs):    
    distinct_datasets = list(dfs.keys())

    master_column_values = list(dfs['Master'].columns)

    dataset_column_map = {dataset_name: list(dfs[dataset_name].columns) for dataset_name in distinct_datasets}
    
    column_pairs = [
        (('Master', m_key, dataset, e_key), dataset, m_key, e_key)
        for dataset in distinct_datasets
        for m_key in master_column_values
        for e_key in dataset_column_map[dataset]
    ]
    return sorted(column_pairs)

# Function to calculate cosine similarity between two vectors
def cosine_similarity(vec1, vec2):
    if not vec1 or not vec2:
        return 0.0
    v1 = np.array(vec1)
    v2 = np.array(vec2)
    return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))

def shannon_entropy(df, colname: str):
    # distinct non-null values as strings
    vals = (
        df.select(col(colname).cast("string").alias("v"))
          .where(col(colname).is_not_null())
          #.distinct()  # TODO: Ask Dr. Ghassemi if he really meant to run this on the "values" not the "series" originally (since all counts would be 1 in the "values")
    )

    # counts per value (will be 1 each because of distinct(), but kept generic)
    counts = vals.group_by("v").agg(count(lit(1)).alias("c"))

    # total count across all values (window over all rows)
    w_all = Window.partition_by()
    counts = counts.with_column("total", sf_sum(col("c")).over(w_all))

    # entropy = -SUM( p * log2(p) ) ; log2(p) = ln(p)/ln(2)
    p = col("c") / col("total")
    entropy_df = counts.select(
        (-sf_sum(p * (ln(p) / ln(lit(2))))).alias("entropy")
    )

    # On empty input, entropy will come back as None
    return entropy_df.collect()[0][0]

def get_fill_rate(df, colname: str):
    return float(df.select(
        avg(when(col(colname).is_not_null(), 1).otherwise(0))
    ).collect()[0][0])

def get_avg_len(df, colname: str):
    return (
            df
            .select(col(colname).cast("string").alias("v"))
            .where(col(colname).is_not_null())
            .distinct()
            .select(avg(length(col("v"))))
            .collect()[0][0]
        )

def precompute_embeddings(meta_data_df, embed_model):
    embedded_df = meta_data_df.select(
        col("\"Dataset\""),
        col("\"Column\""),
        call_function("SNOWFLAKE.CORTEX.EMBED_TEXT_1024", 
                      lit(embed_model), 
                      col("\"Description\"")).alias("EMBEDDING")
    ).collect()
    
    embedding_dict = {
        (row["Dataset"], row["Column"]): row["EMBEDDING"]
        for row in embedded_df
    }
    return embedding_dict

def compute_features(key, dataset, m_key, e_key, meta_data, dfs, results_dict, entropy_dict, embedding_dict, fill_rate_dict, avg_len_dict):
    sub_result = results_dict.get(key, {})

    # Add identifying metadata
    sub_result.setdefault('master_dataset', 'Master')
    sub_result.setdefault('enrichment_dataset', dataset)
    sub_result.setdefault('master_column', m_key)
    sub_result.setdefault('enrichment_column', e_key)

    m_meta = meta_data['Master'][m_key]
    e_meta = meta_data[dataset][e_key]
    m_type = m_meta['Detected Data Type']
    e_type = e_meta['Detected Data Type']

    sub_result.setdefault('master_description', m_meta.get('Description', ''))
    sub_result.setdefault('enrichment_description', e_meta.get('Description', ''))
    sub_result.setdefault('master_data_type', m_type)
    sub_result.setdefault('enrichment_data_type', e_type)
    sub_result.setdefault('master_example_values', m_meta.get('Example Values', []))
    sub_result.setdefault('enrichment_example_values', e_meta.get('Example Values', []))
    sub_result.setdefault('type_mismatch', m_type != e_type)

    # master_series     = precomputed_master[m_key]["series"]
    # enrichment_series = dfs[dataset][e_key]
    # enrichment_values = list(map(str, enrichment_series.dropna().unique()))

    master_df = dfs["Master"]
    enrich_df = dfs[dataset]
    
    master_fill_rate = fill_rate_dict[("Master", m_key)]

    enrichment_fill_rate = fill_rate_dict[(dataset, e_key)]
    
    sub_result.setdefault('master_fill_rate', master_fill_rate)
    sub_result.setdefault('enrichment_fill_rate', enrichment_fill_rate)
    sub_result.setdefault('fuzzy_ratio', fuzz.ratio(m_key, e_key))
    sub_result.setdefault('fuzzy_token_sort_ratio', fuzz.token_sort_ratio(m_key, e_key))
    sub_result.setdefault('fuzzy_token_set_ratio', fuzz.token_set_ratio(m_key, e_key))

    if 'description_embedding_similarity' not in sub_result:
        try:
            vec1 = embedding_dict[('Master', m_key)]
            vec2 = embedding_dict[(dataset, e_key)]
            sub_result['description_embedding_similarity'] = cosine_similarity(vec1, vec2)
        except Exception as e:
            sub_result['description_embedding_similarity'] = None

    # master_values = precomputed_master[m_key]["values"]
    # overlap = set(master_values) & set(enrichment_values)
    # union = set(master_values) | set(enrichment_values)

    # 1-col "sets": DISTINCT non-null values (cast to string to make things match by value over type, just in case)
    m_vals = (
        master_df
        .select(col(m_key).cast("string").alias("v"))
        .where(col(m_key).is_not_null())
        .distinct()
    )
    
    e_vals = (
        enrich_df
        .select(col(e_key).cast("string").alias("v"))
        .where(col(e_key).is_not_null())
        .distinct()
    )
    
    # Counts of unique non-null values
    master_unique_nonnull = m_vals.count()
    enrich_unique_nonnull = e_vals.count()
    
    # Intersection (common distinct values)
    intersection_df = m_vals.intersect(e_vals)
    intersection_count = intersection_df.count()
    
    # Union (distinct values across both)
    union_df = m_vals.union(e_vals).distinct()
    union_count = union_df.count()

    sub_result.setdefault('overlap_master_ratio', intersection_count / master_unique_nonnull if master_unique_nonnull else 0)
    sub_result.setdefault('overlap_enrichment_ratio', intersection_count / enrich_unique_nonnull if enrich_unique_nonnull else 0)
    sub_result.setdefault('overlap_jaccard', intersection_count / union_count if union_count else 0)

    if 'avg_length_diff' not in sub_result:
        # m_lengths = [len(v) for v in master_values]
        # e_lengths = [len(v) for v in enrichment_values]
        m_avg_len = avg_len_dict[("Master", m_key)]
        
        e_avg_len = avg_len_dict[(dataset, e_key)]
        
        avg_length_diff = (
            abs(float(m_avg_len) - float(e_avg_len))
            if m_avg_len is not None and e_avg_len is not None
            else None
        )
        sub_result['avg_length_diff'] = avg_length_diff

    if 'entropy_master' not in sub_result or 'entropy_enrichment' not in sub_result:
        entropy_master = entropy_dict[('Master', m_key)]
        entropy_enrich = entropy_dict[(dataset, e_key)]
        sub_result['entropy_master'] = float(entropy_master) if entropy_master is not None else None
        sub_result['entropy_enrichment'] = float(entropy_enrich) if entropy_enrich is not None else None
        sub_result['entropy_gap'] = abs(float(entropy_master) - float(entropy_enrich)) if entropy_master is not None and entropy_enrich is not None else None

    sub_result['status'] = 'processed'
    return key, sub_result

def extract_features(dbt, session, meta_data, dfs):
    print("------------------------------------------------------------")
    print("Step 3: Computing column-pair features...")
    print("------------------------------------------------------------")
    column_pairs = generate_column_pairs(dfs)
    print(f"Total column pair comparisons: {len(column_pairs)}")

    embed_model = dbt.config.get("config")["embedding_settings"]["model"]
    embedding_dict = precompute_embeddings(meta_data, embed_model)

    # Convert back to the original dict structure
    df = meta_data.to_pandas()
    
    # restore index
    df = df.set_index(["Dataset", "Column"])
    
    # rebuild nested dict
    meta_data = {}
    # Create list of tuples for iterating over columns for entropy precomputation
    entropy_list = []
    for (dataset, column), row in df.iterrows():
        meta_data.setdefault(dataset, {})[column] = row.to_dict()
        this_df = dfs[dataset]
        entropy_list.append((this_df, column, dataset))

    entropy_dict = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(shannon_entropy, df_, col_name): (dataset, col_name)
            for df_, col_name, dataset in entropy_list
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Precomputing entropies for all columns"):
            key = futures[future]
            result = future.result()
            entropy_dict[key] = result

    # Do something similar for fill rates
    fill_rate_dict = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_fill_rate, df_, col_name): (dataset, col_name)
            for df_, col_name, dataset in entropy_list
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Precomputing fill rates for all columns"):
            key = futures[future]
            result = future.result()
            fill_rate_dict[key] = result

    # Do something similar for avg lens
    avg_len_dict = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_avg_len, df_, col_name): (dataset, col_name)
            for df_, col_name, dataset in entropy_list
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Precomputing average lengths for all columns"):
            key = futures[future]
            result = future.result()
            avg_len_dict[key] = result

    results_dict = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(compute_features, key, dataset, m_key, e_key, meta_data, dfs, results_dict, entropy_dict, embedding_dict, fill_rate_dict, avg_len_dict): key
            for key, dataset, m_key, e_key in column_pairs
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Computing features"):
            key, result = future.result()
            results_dict[key] = result

    # print(results_dict)
    
    df = pd.DataFrame(list(results_dict.values()))
    df.columns = df.columns.str.upper()
    session.write_pandas(
        df,
        table_name="STEP3_ALL_COLUMN_PAIR_FEATURES",
        schema="RAW",
        overwrite=True
    )
    print(f"✔ Feature computation complete on {len(results_dict)} column pairs.")
    print(f"✔ Features written to Snowflake\n")
    return dbt.ref("raw_pos_step3_all_column_pair_features")

def model(dbt, session):
    dbt.ref('step2')  # Make it so this runs after step2
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    dfs = load_data(dbt)
    meta_data = dbt.ref("raw_pos_step2_column_metadata_descriptions")
    result_df = extract_features(dbt, session, meta_data, dfs)
    return result_df
