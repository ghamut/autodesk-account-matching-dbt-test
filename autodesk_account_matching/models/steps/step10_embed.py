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

def apply_llm_judgment_on_account_matches(session, matches, llm_model, llm_temp, llm_top_p, llm_max_tokens):
    print("------------------------------------------------------------")
    print("Step 10: Applying final LLM pass to determine best matches...")
    print("------------------------------------------------------------")

    system_preamble = (
        "You are an expert in entity resolution. Respond concisely and accurately.\n\n"
    )

    prompt_col = F.concat(
        F.lit(system_preamble),
        F.lit("You are a data validation expert. Determine if the two account records below represent the same entity. "),
        F.lit("You should assume that each record is complete and internally consistent.\n\n"),
        F.lit("Record A:\n"),
        F.coalesce(F.col("enrichment_evidence").cast("string"), F.lit("")),
        F.lit("\n\nRecord B:\n"),
        F.coalesce(F.col("master_evidence").cast("string"), F.lit("")),
        F.lit("\n\nRespond with one word:\n"),
        F.lit('- "Yes" if they clearly represent the same entity.\n'),
        F.lit('- "No" if they clearly represent different entities.\n'),
        F.lit('- "Maybe" if there is not enough information to make a confident decision.\n\n'),
        F.lit("Then, on the next line, briefly justify your choice.\n"),
    )

    # Deterministic settings; cap tokens so the justification stays short.
    model_parameters_obj = F.object_construct(
        F.lit("temperature"), F.lit(llm_temp),
        F.lit("max_tokens"), F.lit(llm_max_tokens),
        F.lit("top_p"), F.lit(llm_top_p)
    )

    response_schema_obj = F.object_construct(
        F.lit("type"), F.lit("object"),
        F.lit("additionalProperties"), F.lit(False),
        F.lit("properties"), F.object_construct(
            F.lit("decision"), F.object_construct(
                F.lit("type"), F.lit("string"),
                F.lit("enum"), F.array_construct(F.lit("Yes"), F.lit("No"), F.lit("Maybe")),
            ),
            F.lit("justification"), F.object_construct(
                F.lit("type"), F.lit("string"),
            ),
        ),
        F.lit("required"), F.array_construct(F.lit("decision"), F.lit("justification")),
    )

    response_format_obj = F.object_construct(
        F.lit("type"), F.lit("json"),
        F.lit("schema"), response_schema_obj,
    )

    llm_obj = F.call_function(
        "AI_COMPLETE",
        F.lit(llm_model),
        prompt_col,
        model_parameters_obj,
        response_format_obj,
        F.lit(False),  # show_details
    )

    out_df = (
        matches
        .with_column("final_llm_decision", llm_obj["decision"].cast("string"))
        .with_column("final_llm_justification", llm_obj["justification"].cast("string"))
    )
    # Persist results fully in Snowflake (no local materialization).
    out_df.write.mode("overwrite").save_as_table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP10_FINAL_LLM_ROW_MATCHES_EMBED')

    # Small aggregate for the same console summary (only a few rows collected).
    counts_rows = (
        out_df.group_by(F.col("final_llm_decision"))
              .agg(F.count(F.lit(1)).alias("n"))
              .collect()
    )
    counts = {r["FINAL_LLM_DECISION"]: r["N"] for r in counts_rows}

    print(
        "✔ Final LLM judgment complete; "
        f"{counts.get('Yes', 0)} rows confirmed as matches, "
        f"{counts.get('No', 0)} as non-matches, and "
        f"{counts.get('Maybe', 0)} as uncertain."
    )
    print("✔ Final LLM match decisions written to Snowflake\n")

    return session.table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP10_FINAL_LLM_ROW_MATCHES_EMBED')

def model(dbt, session):
    dbt.ref('step7_8_9_embed')  # Make it so this runs after step9
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    matches = session.table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP9_FINAL_TRANSFORMED_DFS_EMBED')

    llm_settings = dbt.config.get("config")["llm_settings"]
    llm_model, llm_temp, llm_top_p, llm_max_tokens = llm_settings['model'], llm_settings['temperature'], llm_settings['top_p'], llm_settings['max_tokens']

    step10_settings = dbt.config.get("config")["step10_llm_calls"]
    llm_top_n, backup_threshold = step10_settings['ask_llm_on_top_n'], step10_settings['backup_threshold']

    top_n = dbt.config.get("config")["feature_thresholds"]["embedding_top_n"]

    if llm_top_n <= 0:
        out_df = (
            matches
            .with_column("final_llm_decision", F.when(F.col("AVG_SIMILARITY") >= backup_threshold, F.lit("Yes")).otherwise(F.lit("No")))
            .with_column("final_llm_justification", F.lit("LLM not called; backup threshold used"))
        )
        out_df.write.mode("overwrite").save_as_table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP10_FINAL_LLM_ROW_MATCHES_EMBED')
        matches = session.table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP10_FINAL_LLM_ROW_MATCHES_EMBED')

    elif 0 < llm_top_n < top_n:
        window_spec = Window.partition_by("MASTER_ROW_INDEX").order_by(F.col("AVG_SIMILARITY").desc())
        matches_ranked = matches.with_column("_rank", F.row_number().over(window_spec))
        
        top_matches = matches_ranked.filter(F.col("_rank") <= llm_top_n).drop("_rank")
        rest_matches = (
            matches_ranked
            .filter(F.col("_rank") > llm_top_n)
            .drop("_rank")
            .with_column("final_llm_decision", F.lit("No"))
            .with_column("final_llm_justification", F.lit("LLM not called; similarity too low"))
        )
        
        llm_results = apply_llm_judgment_on_account_matches(session, top_matches, llm_model, llm_temp, llm_top_p, llm_max_tokens)
        combined = llm_results.union_all(rest_matches)
        combined.write.mode("overwrite").save_as_table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP10_FINAL_LLM_ROW_MATCHES_EMBED')
        matches = session.table('AUTODESK_ACCOUNT_MATCHING_DB.RAW.STEP10_FINAL_LLM_ROW_MATCHES_EMBED')
    
    else:
        matches = apply_llm_judgment_on_account_matches(session, matches, llm_model, llm_temp, llm_top_p, llm_max_tokens)

    return matches
