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

def apply_llm_judgment_on_account_matches(matches):
    print("------------------------------------------------------------")
    print("Step 10: Applying final LLM pass to determine best matches...")
    print("------------------------------------------------------------")

    model = "openai-gpt-4.1"

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
        F.lit("temperature"), F.lit(0),
        F.lit("max_tokens"), F.lit(200),
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
        F.lit(model),
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
    out_df.write.mode("overwrite").save_as_table('AUTODESK_ACCOUNT_MATCHING_DB.RAW."step10_final_llm_row_matches"')

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

    return out_df

def model(dbt, session):
    # TODO: Adjust thresholds to get some Yes values
    dbt.ref('step7_8_9')  # Make it so this runs after step9
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    matches = session.table('AUTODESK_ACCOUNT_MATCHING_DB.RAW."step9_final_transformed_dfs"')
    matches = apply_llm_judgment_on_account_matches(matches)
    return matches
