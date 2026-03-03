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
FT = {
    'max_avg_length_diff': 12,              # Discard columns with large difference in value length
    'min_fill_rate': 0.25,                  # Discard sparse columns
    'min_description_similarity': 0.3,      # Minimum cosine similarity between description embeddings
    'high_similarity_override_threshold': 0.8, # Allows keeping mismatched types if textually very similar
    'max_entropy_gap': 2                    # Penalize columns with very different value diversity
}
FW = {
    'fuzzy_ratio': 25,             # Importance of raw name similarity
    'overlap_jaccard': 100,        # Shared value overlap (set-wise Jaccard index)
    'avg_length_diff': 50,         # Penalize large differences in value length
    'entropy_gap': 10,             # Penalize dissimilar information entropy
    'master_fill_rate': 100,       # Prefer well-populated master fields
    'enrichment_fill_rate': 100    # Prefer well-populated enrichment fields
}

def load_data(dbt):
    print("------------------------------------------------------------")
    print("Step 1: Loading input data...")
    print("------------------------------------------------------------")

    dfs = {
        'Master': dbt.ref("raw_pos_master"),
        'ENR 250': dbt.ref("raw_pos_enr_250"),
        'ENR 400': dbt.ref("raw_pos_enr_400"),
        'ENR 600': dbt.ref("raw_pos_enr_600"),
        'Global data': dbt.ref("raw_pos_global_data"),
    }
    if 'Master' not in dfs:
        raise ValueError("Master dataset not found in loaded data.")
    print(f"✔ Data on {len(dfs)} datasets successfully loaded from Snowflake\n")
    return dfs

def extract_best_column_matches(result_df_filtered, dfs, weight_map=None):
    result_df_filtered = result_df_filtered.to_pandas()
    
    if weight_map is None:
        weight_map = {
            'fuzzy_ratio'.upper(): FW['fuzzy_ratio'],
            'overlap_jaccard'.upper(): FW['overlap_jaccard'],
            'avg_length_diff'.upper(): FW['avg_length_diff'],
            'entropy_gap'.upper(): FW['entropy_gap'],
            'master_fill_rate'.upper(): FW['master_fill_rate'],
            'enrichment_fill_rate'.upper(): FW['enrichment_fill_rate']
        }

    # Step 1: Filter to only rows where ChatGPT said "Yes"
    df = result_df_filtered[result_df_filtered['chatgpt_decision'.upper()] == 'Yes'].copy()

    # Step 2: Compute weighted scores
    grouped = df.groupby(['master_column'.upper(), 'enrichment_dataset'.upper()])
    score_df = (
        grouped[list(weight_map.keys())]
        .mean()
        .mul(pd.Series(weight_map))
        .sum(axis=1)
        .reset_index(name='weighted_score'.upper())
    )
    df = df.merge(score_df, on=['master_column'.upper(), 'enrichment_dataset'.upper()], how='left')

    # Step 3: Keep top match per (master_column, enrichment_dataset)
    best_matches = df.loc[
        df.groupby(['master_column'.upper(), 'enrichment_dataset'.upper()])['weighted_score'.upper()].idxmax()
    ]

    # Step 4: Keep top match per (enrichment_dataset, enrichment_column)
    best_matches_grouped = best_matches.loc[
        best_matches.groupby(['enrichment_dataset'.upper(), 'enrichment_column'.upper()])['weighted_score'.upper()].idxmax()
    ].copy()

    # Step 5: Cluster enrichment columns associated with each master
    master_related = (
        best_matches_grouped[best_matches_grouped['enrichment_dataset'.upper()] == 'Master']
        .groupby('master_column'.upper())['enrichment_column'.upper()]
        .agg(lambda x: sorted(set(x)))
    )

    best_matches_grouped['enrichment_candidates'.upper()] = best_matches_grouped['master_column'.upper()].apply(
        lambda mc: ', '.join(sorted(set(master_related.get(mc, []) + [mc])))
    )

    # Step 6: Compute overlap percentages
    percent_master = []
    percent_enrichment = []
    for _, row in best_matches_grouped.iterrows():
        ec, ed, mc = row['enrichment_column'.upper()], row['enrichment_dataset'.upper()], row['master_column'.upper()]
        # edata = dfs[ed][ec].dropna().unique()
        # mdata = dfs['Master'][mc].dropna().unique()
        # intersection = set(edata) & set(mdata)
        m_vals = (
            dfs['Master']
            .select(col(mc).cast("string").alias("v"))
            .where(col(mc).is_not_null())
            .distinct()
        )
        
        e_vals = (
            dfs[ed]
            .select(col(ec).cast("string").alias("v"))
            .where(col(ec).is_not_null())
            .distinct()
        )
        
        # Counts of unique non-null values
        master_unique_nonnull = m_vals.count()
        enrich_unique_nonnull = e_vals.count()
        
        # Intersection (common distinct values)
        intersection_df = m_vals.intersect(e_vals)
        intersection_count = intersection_df.count()

        percent_master.append(100 * intersection_count / master_unique_nonnull if master_unique_nonnull > 0 else 0)
        percent_enrichment.append(100 * intersection_count / enrich_unique_nonnull if enrich_unique_nonnull > 0 else 0)

    best_matches_grouped['percent_master_in_enrichment'.upper()] = percent_master
    best_matches_grouped['percent_enrichment_in_master'.upper()] = percent_enrichment

    # Step 7: Return best rows per enrichment cluster (excluding enrichment_dataset == 'Master')
    final = (
        best_matches_grouped[best_matches_grouped['enrichment_dataset'.upper()] != 'Master']
        .loc[lambda df: df.groupby(['enrichment_dataset'.upper(), 'enrichment_candidates'.upper()])['weighted_score'.upper()].idxmax()]
    )

    return final[[
        'master_column'.upper(),
        'enrichment_dataset'.upper(),
        'enrichment_column'.upper(),
        'enrichment_candidates'.upper(),
        'weighted_score'.upper(),
        'percent_master_in_enrichment'.upper(),
        'percent_enrichment_in_master'.upper()
    ]]

def get_best_column_matches(dbt, session, result_df_filtered, dfs):     
    print("------------------------------------------------------------")
    print("Step 6: Extracting column-level final matches...")
    print("------------------------------------------------------------")
    final_matches = extract_best_column_matches(result_df_filtered, dfs)
    session.write_pandas(
        final_matches,
        table_name="STEP6_FINAL_COLUMN_MATCHES",
        schema="RAW",
        overwrite=True
    )    
    print(f"✔ {len(final_matches)} Final matches written to Snowflake\n")
    return dbt.ref("raw_pos_step6_final_column_matches")

def model(dbt, session):
    dbt.ref('step5')  # Make it so this runs after step5
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    dfs = load_data(dbt)
    result_df_filtered = dbt.ref("raw_pos_step5_gpt_column_pair_classification")
    final_matches = get_best_column_matches(dbt, session, result_df_filtered, dfs)
    return final_matches
