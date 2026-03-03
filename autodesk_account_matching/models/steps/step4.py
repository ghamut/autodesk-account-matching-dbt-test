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

def filter_results(session, result_df):
    print("------------------------------------------------------------")
    print("Step 4: Filtering out column-pair results...")
    print("------------------------------------------------------------")
    result_df = result_df.to_pandas()
    filtered = result_df[
        (result_df['avg_length_diff'] < FT['max_avg_length_diff']) &
        (result_df['master_fill_rate'] >= FT['min_fill_rate']) &
        (result_df['enrichment_fill_rate'] >= FT['min_fill_rate']) &
        (result_df['description_embedding_similarity'] >= FT['min_description_similarity']) &
        (
            (~result_df['type_mismatch']) |
            (result_df['description_embedding_similarity'] > FT['high_similarity_override_threshold'])
        ) &
        (result_df['entropy_gap'] <= FT['max_entropy_gap']) &
        ~((result_df['master_dataset'] == 'Master') &
          (result_df['enrichment_dataset'] == 'Master') &
          (result_df['master_column'] == result_df['enrichment_column'])) &
        (result_df['master_column'] != result_df['enrichment_column'])
    ].copy()
    session.write_pandas(
        filtered,
        table_name="step4_filtered_column_pairs",
        schema="RAW",
        overwrite=True
    )
    print(f"✔ Filtered from {len(result_df)} → {len(filtered)} candidates.")
    print(f"✔ Filtered results written to Snowflake\n")
    return session.table("AUTODESK_ACCOUNT_MATCHING_DB.RAW.\"step4_filtered_column_pairs\"")

def model(dbt, session):
    dbt.ref('step3')  # Make it so this runs after step3
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    result_df = session.table("AUTODESK_ACCOUNT_MATCHING_DB.RAW.\"step3_all_column_pair_features\"")
    result_df_filtered = filter_results(session, result_df)
    return result_df_filtered
