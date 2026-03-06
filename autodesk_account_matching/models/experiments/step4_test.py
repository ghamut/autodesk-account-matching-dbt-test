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

def filter_results(dbt, session, result_df, FT):
    print("------------------------------------------------------------")
    print("Step 4: Filtering out column-pair results...")
    print("------------------------------------------------------------")
    result_df = result_df.to_pandas()
    filtered = result_df[
        (result_df['avg_length_diff'.upper()] < FT['max_avg_length_diff']) &
        (result_df['master_fill_rate'.upper()] >= FT['min_fill_rate']) &
        (result_df['enrichment_fill_rate'.upper()] >= FT['min_fill_rate']) &
        (result_df['description_embedding_similarity'.upper()] >= FT['min_description_similarity']) &
        (
            (~result_df['type_mismatch'.upper()]) |
            (result_df['description_embedding_similarity'.upper()] > FT['high_similarity_override_threshold'])
        ) &
        (result_df['entropy_gap'.upper()] <= FT['max_entropy_gap']) &
        ~((result_df['master_dataset'.upper()] == 'Master') &
          (result_df['enrichment_dataset'.upper()] == 'Master') &
          (result_df['master_column'.upper()] == result_df['enrichment_column'.upper()])) &
        (result_df['master_column'.upper()] != result_df['enrichment_column'.upper()])
    ].copy()
    filtered.columns = filtered.columns.str.upper()
    session.write_pandas(
        filtered,
        table_name="STEP4_FILTERED_COLUMN_PAIRS",
        schema="TEST",
        overwrite=True
    )
    print(f"✔ Filtered from {len(result_df)} → {len(filtered)} candidates.")
    print(f"✔ Filtered results written to Snowflake\n")
    return session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.STEP4_FILTERED_COLUMN_PAIRS")

def model(dbt, session):
    dbt.ref('step3_test')  # Make it so this runs after step3
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    result_df = session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.STEP3_ALL_COLUMN_PAIR_FEATURES")
    FT = dbt.config.get("config")["feature_thresholds"]
    result_df_filtered = filter_results(dbt, session, result_df, FT)
    return result_df_filtered
