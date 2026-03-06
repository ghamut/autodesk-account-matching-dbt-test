import snowflake.snowpark as snowpark
from snowflake.cortex import Complete, CompleteOptions
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

def load_data(session):
    print("------------------------------------------------------------")
    print("Step 1: Loading input data...")
    print("------------------------------------------------------------")

    dfs = {
        'Master': session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.MASTER_ORIGINAL"),
        'Same Columns': session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.MASTER_SAME_COLUMNS_20_MATCHES"),
        'Similar Columns': session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.MASTER_SIMILAR_COLUMNS_20_MATCHES"),
        'Unrelated Columns': session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.MASTER_UNRELATED_COLUMNS_20_MATCHES")
    }
    if 'Master' not in dfs:
        raise ValueError("Master dataset not found in loaded data.")
    print(f"✔ Data on {len(dfs)} datasets successfully loaded from Snowflake\n")
    return dfs

def _quote_ident(name):
    # Snowflake identifier quoting; doubles internal quotes
    return '"' + name.replace('"', '""') + '"'

def make_json_safe(x, dtype):
    if isinstance(x, str) and dtype == 'numerical':
        try:
            if "." in x:
                return float(x)
            return int(x)
        except ValueError:
            pass
    
    try:
        json.dumps(x)
        return x
    except TypeError:
        try:
            return float(x)
        except (TypeError, ValueError):
            return str(x)

def generate_column_stats(table_df, most_common=10, sample_n=1_000):
    # This function should never pull a column/table into Python memory; the sampling is here just to speed things up since we likely don't need to have the entire column to get these specific stats reasonably.
    col_info = {}

    # Random sample up to `sample_n` rows
    sampled_df = table_df.sample(n=sample_n)

    for col in sampled_df.columns:
        qcol = col

        # Force string input so TRY_* works consistently across underlying column types
        as_varchar = f"TO_VARCHAR({qcol})"

        counts = (
            sampled_df.select(
                F.count(F.col(col)).alias("nonnull"),
                F.count(F.sql_expr(f"TRY_TO_DOUBLE({as_varchar})")).alias("num_ok"),
                F.count(F.sql_expr(f"TRY_TO_TIMESTAMP_NTZ({as_varchar})")).alias("ts_ok"),
            )
            .collect()
        )[0]

        nonnull = int(counts["NONNULL"])
        num_ok = int(counts["NUM_OK"])
        ts_ok = int(counts["TS_OK"])

        dtype = (
            "numerical" if nonnull > 0 and num_ok == nonnull
            else "datetime" if nonnull > 0 and ts_ok == nonnull
            else "unknown" if nonnull == 0
            else "text"
        )

        top_rows = (
            sampled_df.where(F.col(col).is_not_null())
            .group_by(F.col(col))
            .agg(F.count(F.lit(1)).alias("cnt"))
            .sort(F.col("cnt").desc())
            .limit(most_common)
            .collect()
        )
        top_values = [make_json_safe(r[0], dtype) for r in top_rows]

        col_info[col] = {
            "Detected Data Type": dtype,
            "Example Values": top_values,
        }

    return col_info

def describe_dataset(dataset, meta_data, llm_model, llm_temp, llm_top_p, llm_max_tokens):
    prompt = """Do not use code to answer the question. Provide only code as your response, no explanations. Do not use markdown formatting.
You will be provided with metadata for a set of columns in a dataset.
Return a dictionary with one key for each column in the dataset. 
For each column, provide a brief description given the Column Name, Example Values, and your best judgement.
Prioritize the Example Values when generating the description, and use the Column Name as a secondary reference.
Example output: {"<column1_name>": "<description1>", "<column2_name>": "<description2>"}
The metadata follows:
"""+f"""```json
{json.dumps(meta_data[dataset])}
```"""
    response = Complete(model=llm_model, prompt=prompt, options=CompleteOptions(temperature=llm_temp, top_p=llm_top_p, max_tokens=llm_max_tokens))
    dict_response = json.loads(response.replace('```json', '').replace('```','').strip())
    for k, v in dict_response.copy().items():
        dict_response[f'"{k}"'] = v
    return dataset, dict_response

def generate_metadata(dbt, session, dfs):
    print("------------------------------------------------------------")
    print("Step 2: Generating column metadata and descriptions...")
    print("------------------------------------------------------------")
    step2_sampling_settings = dbt.config.get("config")["step2_sampling"]
    meta_data = {name: generate_column_stats(df, step2_sampling_settings["num_examples"], step2_sampling_settings["num_samples_for_typing"]) for name, df in dfs.items()}
    llm_settings = dbt.config.get("config")["llm_settings"]
    llm_model, llm_temp, llm_top_p, llm_max_tokens = llm_settings['model'], llm_settings['temperature'], llm_settings['top_p'], llm_settings['max_tokens']
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(describe_dataset, dataset, meta_data, llm_model, llm_temp, llm_top_p, llm_max_tokens): dataset
            for dataset in meta_data
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Describing columns"):
            dataset, descs = future.result()
            for col in meta_data[dataset]:
                meta_data[dataset][col]['Description'] = descs[col]

    df = pd.DataFrame.from_dict(
        {(i, j): meta_data[i][j] for i in meta_data for j in meta_data[i]},
        orient='index'
    )
    # Make the MultiIndex explicit (nice names), then flatten into columns
    df.index = pd.MultiIndex.from_tuples(df.index, names=["Dataset", "Column"])
    df = df.reset_index()  # adds Dataset + Column columns; keeps existing columns unchanged
    df = df.drop(columns=["index"], errors="ignore")
    df = df.astype("string")
    session.write_pandas(
        df,
        table_name="STEP2_COLUMN_METADATA_DESCRIPTIONS",
        schema="TEST",
        overwrite=True
    )
    print(f"✔ Metadata and descriptions written to Snowflake\n")
    return session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.STEP2_COLUMN_METADATA_DESCRIPTIONS")

def model(dbt, session):
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    dfs = load_data(session)
    meta_data = generate_metadata(dbt, session, dfs)
    return meta_data
    