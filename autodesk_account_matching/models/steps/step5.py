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

# Now, for each row in the result_df_filtered, we will provide ChatGPT with a pair of columns, examplee values, and their descriptions; we will ask it to indicate if the two columns are the same or not.
def generate_chatgpt_prompt(row):
    prompt = f"""Below are two columns. Determine if the they represent the same general topic or not (responding with "Yes" or "No"). Below are examples of fields that should be considered as belonging to the "Same" topic:
- Fields that uniquely identify a company name include: Company Name, Parent Company Name, Previous/Former Company Names, Stock Ticker Symbol, Website Domain.
- Fields that uniquely identify a person: Person Name, Full Name, etc.
- Fields that describe the same geographic resolution: Address matches Address, City matches City, State matches State, Zip Code matches Zip Code, etc.
- Fields that describe a unique company identifier, including: D-U-N-S Number (Dun & Bradstreet), LEI (Legal Entity Identifier), Company Registration Number, Tax Identification Number (TIN / EIN), Bloomberg Ticker / FIGI, LinkedIn Company ID, OpenCorporates ID, etc.

You should base your decision primarily on the example values; use the descriptions and name only when the example values are not sufficient to compare the columns. 
Please classify the two fields as "Same" if they represent the same kind of information, regardless of data versions, small differences in naming conventions, abbreviations, formatting, casing, or regional spellings.
Base your judgment on whether both columns fundamentally represent the same topic, even if their values are not the same.
Also provide a brief justification for your answer on the next line.

Assume that each column is internally consistent:
- All values within a single column always represent the same type of information (e.g., all company names, or all person names, never a mix of the two).
- If the example values in a column appear inconsistent or ambiguous, you must consider the most likely topic for that column based on the entire set of example values provided.
- Never infer that a column mixes topics; always assume each column represents one, and only one topic.

Column 1:
- Name: {row['master_column'.upper()]}
- Example Values: {', '.join(map(str, row['master_example_values'.upper()]))}

Column 2:
- Name: {row['enrichment_column'.upper()]}
- Example Values: {', '.join(map(str, row['enrichment_example_values'.upper()]))}
 
Format your response exactly like this:
"Yes" or "No"
Justification here."""
    return prompt

def ask_chatgpt(prompt, system, llm_model, llm_temp, llm_top_p, llm_max_tokens):
    response = Complete(model=llm_model, prompt=f'{system}\n{prompt}', options=CompleteOptions(temperature=llm_temp, top_p=llm_top_p, max_tokens=llm_max_tokens))
    return response.strip()

def gpt_match_row(index_row, llm_model, llm_temp, llm_top_p, llm_max_tokens):
    index, row = index_row
    prompt = generate_chatgpt_prompt(row)
    response = ask_chatgpt(prompt, "You are an expert in data matching. Provide only 'Yes' or 'No' as your response.", llm_model, llm_temp, llm_top_p, llm_max_tokens)
    lines = response.splitlines()
    return {
        "index": index,
        "decision": lines[0].strip() if lines else "",
        "justification": lines[1].strip() if len(lines) > 1 else ""
    }

def apply_gpt_decision(dbt, session, result_df_filtered):
    print("------------------------------------------------------------")
    print("Step 5: Applying GPT judgment to remaining column matches...")
    print("------------------------------------------------------------")
    result_df_filtered = result_df_filtered.to_pandas()
    rows = list(result_df_filtered.iterrows())
    llm_settings = dbt.config.get("config")["llm_settings"]
    llm_model, llm_temp, llm_top_p, llm_max_tokens = llm_settings['model'], llm_settings['temperature'], llm_settings['top_p'], llm_settings['max_tokens']
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(gpt_match_row, row, llm_model, llm_temp, llm_top_p, llm_max_tokens): i for i, row in enumerate(rows)}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Evaluating pairs via GPT"):
            result = future.result()
            result_df_filtered.at[result["index"], 'chatgpt_decision'.upper()] = result["decision"]
            result_df_filtered.at[result["index"], 'chatgpt_justification'.upper()] = result["justification"]

    result_df_filtered.columns = result_df_filtered.columns.str.upper()
    session.write_pandas(
        result_df_filtered,
        table_name="STEP5_GPT_COLUMN_PAIR_CLASSIFICATION",
        schema="RAW",
        overwrite=True
    )    
    print(f"✔ GPT assessment complete; {result_df_filtered['chatgpt_decision'.upper()].value_counts().get('Yes', 0)} columns were determined to match.")
    print(f"✔ GPT decisions written to Snowflake\n")
    return dbt.ref("raw_pos_step5_gpt_column_pair_classification")

def model(dbt, session):
    dbt.ref('step4')  # Make it so this runs after step4
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','rapidfuzz','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    result_df_filtered = dbt.ref("raw_pos_step4_filtered_column_pairs")
    result_df_filtered = apply_gpt_decision(dbt, session, result_df_filtered)
    return result_df_filtered
