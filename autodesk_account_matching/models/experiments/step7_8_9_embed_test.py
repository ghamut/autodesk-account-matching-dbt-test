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
import numpy as np
from langdetect import detect_langs
import re

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

def ask_chatgpt(prompt, system, llm_model, llm_temp, llm_top_p, llm_max_tokens):
    response = Complete(model=llm_model, prompt=f'{system}\n{prompt}', options=CompleteOptions(temperature=llm_temp, top_p=llm_top_p, max_tokens=llm_max_tokens))
    return response.strip()

def find_filter_columns(matches_df, overlap_threshold):
    # Basicaly, if the percent overlap between the enrichment and master columns is above the threshold, we will keep it as a filter field.
    # A filter field is a pair of columns that we will use to filter out contradictory rows in the cross-dataset pairs.
    # A mapped field is a pair of columns that we will use to match rows between the master and enrichment datasets.
    matches_df = matches_df.to_pandas()
    filter_fields, mapped_fields = {}, {}
    for _, row in matches_df.iterrows():
        pair = (row['enrichment_column'.upper()], row['master_column'.upper()])
        mapped_fields.setdefault(row['enrichment_dataset'.upper()], []).append(pair)
        if (
            row['percent_enrichment_in_master'.upper()] >= overlap_threshold or
            row['percent_master_in_enrichment'.upper()] >= overlap_threshold
        ):
            filter_fields.setdefault(row['enrichment_dataset'.upper()], []).append(pair)
    return mapped_fields, filter_fields

def _safe_table_suffix(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", s)

def translate_non_english_columns(session, dfs, mapped_fields_by_dataset, translation_llm_model):
    seen = set()
    out = dict(dfs)
    created_temp_tables = []

    for dataset, mappings in mapped_fields_by_dataset.items():
        for e_col, m_col in mappings:
            for dset, c in [(dataset, e_col), ("Master", m_col)]:
                if (dset, c) in seen:
                    continue
                seen.add((dset, c))

                df = out[dset]
                c_str = col(c).cast("string")

                # 1) distinct non-null values in this column
                distinct_vals = (
                    df.select(c_str.alias("v"))
                      .where(col(c).is_not_null())
                      .distinct()
                )

                # 2) keep only values that are NOT English by the existing logic
                to_translate = distinct_vals.where(
                    ~call_function(
                        "AUTODESK_ACCOUNT_MATCHING_DB.RAW.IS_ENGLISH_LANGDETECT_08",
                        col("v")
                    )
                )

                # 3) translate those values once
                translations = to_translate.select(
                    col("v").alias("v"),
                    sql_expr(
                        f"""
                        AI_COMPLETE(
                            '{translation_llm_model}',
                            'Forget all previous instructions. '
                            || 'You are a translation expert. Your task is to translate the following text into English: '
                            || v
                            || ' Please provide only the translated text, without any additional explanations or formatting.'
                        )
                        """
                    ).alias("v_en")
                )

                # 4) join back; replace only when a translation exists
                #    keep nulls as-is
                df_translated = (
                    df.join(translations, c_str == translations["v"], how="left")
                      .with_column(c, coalesce(col("v_en"), col(c)))
                      .drop("v", "v_en")
                )

                # 5) materialize after each column to break lineage
                safe_dset = _safe_table_suffix(dset)
                safe_col = _safe_table_suffix(c)
                temp_table = f"AUTODESK_ACCOUNT_MATCHING_DB.TEST.TMP_STEP7_{safe_dset}_{safe_col}"

                df_translated.write.mode("overwrite").save_as_table(temp_table)
                created_temp_tables.append(temp_table)

                # Reload from materialized result
                out[dset] = session.table(temp_table)

    return out, created_temp_tables

def translate_nonenglish_entries(session, dfs, final_matches, translation_llm_model, overlap_threshold):
    print("------------------------------------------------------------")
    print("Step 7: Translating non-English values in matched fields...")
    print("------------------------------------------------------------")

    mapped_fields_by_dataset, _ = find_filter_columns(final_matches, overlap_threshold)
    dfs, created_temp_tables = translate_non_english_columns(
        session, dfs, mapped_fields_by_dataset, translation_llm_model
    )

    dfs_dict = {}

    # First write the final outputs
    for name, df in dfs.items():
        safe_name = name.replace(" ", "_")
        new_table = f"STEP7_TRANSLATED_DFS_{safe_name}_EMBED"
        full_table_name = f"AUTODESK_ACCOUNT_MATCHING_DB.TEST.{new_table}"

        df.write.mode("overwrite").save_as_table(full_table_name)
        dfs_dict[name] = session.table(full_table_name)

    # Then clean up intermediates
    for temp_table in set(created_temp_tables):
        try:
            session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
        except Exception as e:
            print(f"Warning: failed to drop temp table {temp_table}: {e}")

    print("✔ Translated DataFrames written to Snowflake")
    print("✔ Temporary tables cleaned up\n")

    return dfs_dict

def normalize(text):
    if pd.isna(text): return ""
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', str(text).lower().strip()))

def apply_column_transforms(session, dfs, final_matches, llm_model, llm_temp, llm_top_p, llm_max_tokens, overlap_threshold):
    print("\nStep 8: Applying transformations to the DataFrames based on the final matches.")

    mapped_fields_by_dataset, filter_fields_by_dataset = find_filter_columns(final_matches, overlap_threshold)

    out = dict(dfs)

    for dataset, mappings in mapped_fields_by_dataset.items():
        for e_col, m_col in mappings:

            top_50_master_df = (
                dfs["Master"]
                .select(col(m_col).cast("string").alias("v"))
                .where(col(m_col).is_not_null())
                .group_by("v")
                .agg(count("*").alias("cnt"))
                .sort(col("cnt").desc())
                .limit(50)
            )
            most_common_master = [row[0] for row in top_50_master_df.collect()]

            top_50_enrichment_df = (
                dfs[dataset]
                .select(col(e_col).cast("string").alias("v"))
                .where(col(e_col).is_not_null())
                .group_by("v")
                .agg(count("*").alias("cnt"))
                .sort(col("cnt").desc())
                .limit(50)
            )
            most_common_enrichment = [row[0] for row in top_50_enrichment_df.collect()]

            normalized_master = [normalize(v) for v in most_common_master]
            normalized_enrichment = [normalize(v) for v in most_common_enrichment]

            prompt = f"""Below are the names and values of two columns that contain the same type of data, but not necessarily the same values, in the same format. You will help me pre-process this data for approximate matching. Specifically, your task is to review the examples and generate a general-purpose Python function called `transform(value)` that modifies values from the enrichment column to match the format and structure of values from the master column to facilitate matching by me. If a transformation is not relevant for matching, don't do it.

Do not return a rigid mapping of values that work only for the examples provided; rather, you must return a general transformation function that can be applied to any value in the enrichment column including those not shown in the examples. You may use a small suffix list, but it must be deduplicated and stripped of punctuation and casing before use.

Master Column Example Values: {', '.join(normalized_master)}
Enrichment Column Example Values: {', '.join(normalized_enrichment)}

Your output must be a single Python function called `transform(value)`, which implements generalizable transformations. Do not return a dict of naive transformations that will only work on the examples provided. If no meaningful or general transformation can be determined, return `None`. Your complete function must not exceed 1000 characters. You cannot use any Python packages that are not included in the Python standard library (i.e., require installation such as with pip). Your function must return a single string."""

            response = ask_chatgpt(
                prompt,
                "Provide only Python code as your response. No explanations. No markdown formatting.",
                llm_model, llm_temp, llm_top_p, llm_max_tokens
            )

            cleaned_response = (
                response.strip()
                .removeprefix("```python")
                .removesuffix("```")
                .strip()
            )

            transform_func = None
            if "def transform(" not in cleaned_response:
                print(f"  ❌ Skipping: response does not define `transform()` function.")
            else:
                for attempt in range(2):
                    try:
                        exec_env = {}
                        exec(cleaned_response, exec_env)
                        transform_func = exec_env.get("transform", None)
                        break
                    except Exception as e:
                        print(f"  ❌ Error executing transformation function for {e_col}: {e}")
                        break

            if callable(transform_func):
                try:
                    dollars = '$'+'$'
                    fn = f"TEMP_FUNCTION_{dataset.upper().replace(' ', '_')}_{e_col.upper().replace(' ', '_')}_{m_col.upper().replace(' ', '_')}"
                    create_udf_sql = f"""
CREATE OR REPLACE TEMP FUNCTION {fn}(text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'transform'
AS
{dollars}
{cleaned_response}

{dollars};"""
                    session.sql(create_udf_sql).collect()
                    out[dataset] = out[dataset].with_column(
                        f"{e_col}__transformed".upper(),
                        when(col(e_col).is_null(), None)
                          .otherwise(call_function(fn, col(e_col).cast("string")))
                    )
                    print(f"  ✅ Transformation function applied successfully for {e_col} -> {m_col}.")

                except Exception as e:
                    print(f"  ❌ Failed to apply transform function to column {e_col}: {e}")
            else:
                print(f"  ⚠️ No valid transformation function returned for {e_col} -> {m_col}.")

    return out

def apply_gpt_column_transforms(session, dfs, final_matches, llm_model, llm_temp, llm_top_p, llm_max_tokens, overlap_threshold):
    print("------------------------------------------------------------")
    print("Step 8: Applying column transformation functions...")
    print("------------------------------------------------------------")
    dfs = apply_column_transforms(session, dfs, final_matches, llm_model, llm_temp, llm_top_p, llm_max_tokens, overlap_threshold)
    dfs_dict = {}

    for name, df in dfs.items():
        safe_name = name.replace(" ", "_")
        new_table = f"STEP8_TRANSFORMED_DFS_{safe_name}_EMBED"

        df.write.mode("overwrite").save_as_table(
            f"AUTODESK_ACCOUNT_MATCHING_DB.TEST.{new_table}"
        )

        dfs_dict[name] = session.table(
            f"AUTODESK_ACCOUNT_MATCHING_DB.TEST.{new_table}"
        )

    print(f"✔ Transformed DataFrames written to Snowflake\n")
    return dfs_dict

def filter_rows(dfs, dataset, mappings, filters=None):
    filters = filters or []

    df_e_base = dfs[dataset]
    df_m_base = dfs["Master"]

    # Add row indexes (do not materialize data in Python)
    w = Window.order_by(F.lit(1))
    df_e_idx = df_e_base.with_column("ENRICHMENT_ROW_INDEX", F.row_number().over(w))
    df_m_idx = df_m_base.with_column("MASTER_ROW_INDEX", F.row_number().over(w))

    e_cols = df_e_base.columns
    m_cols = df_m_base.columns

    # Use safe internal aliases for ALL columns; preserve original names only for display later
    e_final = {c: _internal_col_name(c, "E") for c in e_cols}
    m_final = {c: _internal_col_name(c, "M") for c in m_cols}

    # Build the paired rows (blocking join if filters; else full pairing)
    if filters:
        e_block, m_block = filters[0]
        paired = df_e_idx.join(df_m_idx, df_e_idx[e_block] == df_m_idx[m_block], how="inner")
    else:
        # Same as indexer.full(): all pairs; can be enormous
        paired = df_e_idx.cross_join(df_m_idx)

    paired = paired.select(
        F.col("ENRICHMENT_ROW_INDEX"),
        F.col("MASTER_ROW_INDEX"),
        *[df_e_idx[c].as_(e_final[c]) for c in e_cols],
        *[df_m_idx[c].as_(m_final[c]) for c in m_cols],
    )

    # Filter out contradictory rows for remaining filters (skip first; already used for blocking)
    if filters and len(filters) > 1:
        cond = F.lit(True)
        for e_col, m_col in filters[1:]:
            e_name = e_final[e_col]
            m_name = m_final[m_col]

            val_e = F.lower(F.trim(F.col(e_name).cast("string")))
            val_m = F.lower(F.trim(F.col(m_name).cast("string")))

            cond = cond & (
                F.col(e_name).is_null() |
                F.col(m_name).is_null() |
                (val_e == val_m)
            )

        paired = paired.filter(cond)

    return paired

def _safe_ident_piece(s: str, max_len: int = 80) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:max_len] if len(s) > max_len else s

def _internal_col_name(col_name: str, source_tag: str, max_len: int = 200) -> str:
    """
    Generate a safe internal alias for Snowpark operations.
    Keeps original column names out of intermediate identifiers so quoted/lowercase/space-containing
    source columns do not break later F.col(...) references.
    """
    base = _safe_ident_piece(col_name, max_len=max_len - len(source_tag) - 2)
    if not base:
        base = "COL"
    return f"{base}__{source_tag}".upper()

def _normalize_expr(col_expr):
    s = F.coalesce(col_expr.cast("string"), F.lit(""))
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"[^\w\s]", "")
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.trim(s)
    return s

def _build_embedding_lookup(session, df_values, table_fqn, embed_model):
    # df_values: single column DF with column name "NORM"
    # table_fqn: fully qualified table name
    df_emb = df_values.select(
        F.col("NORM"),
        embed_text_1024(embed_model, F.col("NORM")).as_("EMB"),
    )
    df_emb.write.mode("overwrite").save_as_table(table_fqn)
    return session.table(table_fqn)

def collect_matching_rows(session, dfs, df_cross, mappings, dataset, filter_fields_by_dataset, top_n, embed_model):
    filters_for_dataset = filter_fields_by_dataset.get(dataset, []) or []
    non_filters = [pair for pair in mappings if pair not in filters_for_dataset]

    if not non_filters:
        return df_cross.limit(0)

    # Precompute embedding lookups for each compared column (distinct normalized strings),
    # then join vectors onto df_cross; cosine similarity uses precomputed vectors.
    score_col_names = []
    score_exprs = []

    df_work = df_cross

    for e_col, m_col in non_filters:
        # Use safe internal aliases, not original source names
        e_name = _internal_col_name(e_col, "E")
        m_name = _internal_col_name(m_col, "M")

        e_norm_col = f"NORM__E__{_safe_ident_piece(dataset)}__{_safe_ident_piece(e_col)}".upper()
        m_norm_col = f"NORM__M__{_safe_ident_piece(m_col)}".upper()

        df_work = df_work.with_column(e_norm_col, _normalize_expr(F.col(e_name)))
        df_work = df_work.with_column(m_norm_col, _normalize_expr(F.col(m_name)))

        # Build enrichment embedding lookup for this dataset/column
        enr_lookup_table = (
            f"AUTODESK_ACCOUNT_MATCHING_DB.TEST."
            f"STEP9_EMB_LOOKUP_ENR_{_safe_ident_piece(dataset)}_{_safe_ident_piece(e_col)}_EMBED"
        )
        enr_vals = (
            dfs[dataset]
            .select(_normalize_expr(F.col(e_col)).as_("NORM"))
            .where(F.col(e_col).is_not_null())
            .where(F.col("NORM") != F.lit(""))
            .distinct()
        )
        enr_lookup = _build_embedding_lookup(session, enr_vals, enr_lookup_table, embed_model)

        # Build master embedding lookup for this master column
        mst_lookup_table = (
            f"AUTODESK_ACCOUNT_MATCHING_DB.TEST."
            f"STEP9_EMB_LOOKUP_MASTER_{_safe_ident_piece(m_col)}_EMBED"
        )
        mst_vals = (
            dfs["Master"]
            .select(_normalize_expr(F.col(m_col)).as_("NORM"))
            .where(F.col(m_col).is_not_null())
            .where(F.col("NORM") != F.lit(""))
            .distinct()
        )
        mst_lookup = _build_embedding_lookup(session, mst_vals, mst_lookup_table, embed_model)

        # Join embeddings onto the cross DF
        enr_emb_col = f"EMB__E__{_safe_ident_piece(dataset)}__{_safe_ident_piece(e_col)}".upper()
        mst_emb_col = f"EMB__M__{_safe_ident_piece(m_col)}".upper()

        df_work = (
            df_work
            .join(
                enr_lookup.select(
                    F.col("NORM").as_(f"{e_norm_col}__K"),
                    F.col("EMB").as_(enr_emb_col)
                ),
                df_work[e_norm_col] == F.col(f"{e_norm_col}__K"),
                how="left"
            )
            .drop(f"{e_norm_col}__K")
        )

        df_work = (
            df_work
            .join(
                mst_lookup.select(
                    F.col("NORM").as_(f"{m_norm_col}__K"),
                    F.col("EMB").as_(mst_emb_col)
                ),
                df_work[m_norm_col] == F.col(f"{m_norm_col}__K"),
                how="left"
            )
            .drop(f"{m_norm_col}__K")
        )

        both_present = F.col(e_name).is_not_null() & F.col(m_name).is_not_null()
        score_name = f"SCORE__{_safe_ident_piece(m_col)}".upper()
        score_col_names.append(score_name)

        score_exprs.append(
            F.iff(
                both_present & F.col(enr_emb_col).is_not_null() & F.col(mst_emb_col).is_not_null(),
                call_function("VECTOR_COSINE_SIMILARITY", F.col(enr_emb_col), F.col(mst_emb_col)),
                F.lit(None).cast("float"),
            ).as_(score_name)
        )

    df_scored = df_work.select("*", *score_exprs)

    # Keep ORIGINAL column names in evidence text
    enrichment_parts = [
        F.concat(
            F.lit(f"{m}: "),
            F.coalesce(F.col(_internal_col_name(e, "E")).cast("string"), F.lit("nan")),
        )
        for e, m in mappings
    ]
    master_parts = [
        F.concat(
            F.lit(f"{m}: "),
            F.coalesce(F.col(_internal_col_name(m, "M")).cast("string"), F.lit("nan")),
        )
        for _, m in mappings
    ]

    similarity_raw = F.array_construct(*[F.col(sc) for sc in score_col_names]).as_("SIMILARITY_RAW")

    df_out = df_scored.select(
        F.lit("Master").as_("MASTER_DATASET"),
        F.lit(dataset).as_("ENRICHMENT_DATASET"),
        F.concat_ws(F.lit("; "), *enrichment_parts).as_("ENRICHMENT_EVIDENCE"),
        F.concat_ws(F.lit("; "), *master_parts).as_("MASTER_EVIDENCE"),
        similarity_raw,
        F.col("ENRICHMENT_ROW_INDEX"),
        F.col("MASTER_ROW_INDEX"),
    )

    df_flat = df_out.join_table_function("flatten", F.col("SIMILARITY_RAW"))
    df_flat = df_flat.filter(F.col("VALUE").is_not_null())

    df_final = (
        df_flat.group_by("ENRICHMENT_ROW_INDEX", "MASTER_ROW_INDEX")
        .agg(
            F.any_value("MASTER_DATASET").as_("MASTER_DATASET"),
            F.any_value("ENRICHMENT_DATASET").as_("ENRICHMENT_DATASET"),
            F.any_value("ENRICHMENT_EVIDENCE").as_("ENRICHMENT_EVIDENCE"),
            F.any_value("MASTER_EVIDENCE").as_("MASTER_EVIDENCE"),
            F.array_agg(F.col("VALUE").cast("float")).within_group(F.col("INDEX")).as_("SIMILARITY"),
            F.avg(F.col("VALUE").cast("float")).as_("AVG_SIMILARITY"),
            F.count(F.lit(1)).as_("SIMILARITY_COUNT"),
        )
        .filter(F.col("SIMILARITY_COUNT") > F.lit(0))
        .drop("SIMILARITY_COUNT")
    )

    w_topn = Window.partition_by(F.col("ENRICHMENT_ROW_INDEX")).order_by(F.col("AVG_SIMILARITY").desc_nulls_last())
    df_final = (
        df_final
        .with_column("RN", F.row_number().over(w_topn))
        .filter(F.col("RN") <= F.lit(int(top_n)))
        .drop("RN")
    )

    return df_final

def embed_match_rows(session, dfs, final_matches, overlap_threshold, top_n, embed_model):
    mapped_fields_by_dataset, filter_fields_by_dataset = find_filter_columns(final_matches, overlap_threshold)

    print(
        f"The set intersection of the following columns was above overlap threshold in config ({overlap_threshold} %); "
        "hence the columns are probably formatted the same way; thus should not bother to compare any rows where these column pairs differ."
    )
    for dataset, filters in filter_fields_by_dataset.items():
        print(f"{dataset}: {filters}")

    out_df = None

    for dataset, mappings in mapped_fields_by_dataset.items():
        df_cross = filter_rows(dfs, dataset, mappings, filter_fields_by_dataset.get(dataset, []))
        df_matches = collect_matching_rows(
            session,
            dfs,
            df_cross,
            mappings,
            dataset,
            filter_fields_by_dataset,
            top_n,
            embed_model
        )
        out_df = df_matches if out_df is None else out_df.union_all(df_matches)

    if out_df is None:
        any_df = next(iter(dfs.values()))
        out_df = any_df.limit(0).select(
            F.lit("Master").as_("MASTER_DATASET"),
            F.lit(None).cast("string").as_("ENRICHMENT_DATASET"),
            F.lit(None).cast("string").as_("ENRICHMENT_EVIDENCE"),
            F.lit(None).cast("string").as_("MASTER_EVIDENCE"),
            F.lit(None).cast("array").as_("SIMILARITY"),
            F.lit(None).cast("float").as_("AVG_SIMILARITY"),
            F.lit(None).cast("number").as_("ENRICHMENT_ROW_INDEX"),
            F.lit(None).cast("number").as_("MASTER_ROW_INDEX"),
        )

    return out_df

def get_embed_match_rows(dbt, session, dfs, final_matches, overlap_threshold, top_n, embed_model):
    # TODO: Ensure the ordering of indices is deterministic across runs (likely add another column to all datasets for an index that can be used)
    print("------------------------------------------------------------")
    print("Step 9: Matching rows across datasets with embeddings...")
    print("------------------------------------------------------------")

    matches_df = embed_match_rows(
        session,
        dfs,
        final_matches,
        overlap_threshold,
        top_n,
        embed_model
    )

    matches_df = matches_df.sort(
        F.col("AVG_SIMILARITY").desc_nulls_last(),
    )

    target_table = "AUTODESK_ACCOUNT_MATCHING_DB.TEST.STEP9_FINAL_TRANSFORMED_DFS_EMBED"
    matches_df.write.mode("overwrite").save_as_table(target_table)

    candidate_count = matches_df.count()
    print(f"✔ Matching complete; {candidate_count} candidate row matches found.")
    print("✔ Final transformed DataFrames with matches written to Snowflake\n")

    return session.table(target_table)

def model(dbt, session):
    dbt.ref('step6_test')  # Make it so this runs after step6
    dbt.config(
        packages=['snowflake-snowpark-python','pandas','tqdm','httpx','langdetect','snowflake-ml-python'],
        python_version="3.11"
    )
    dfs = load_data(session)
    final_matches = session.table("AUTODESK_ACCOUNT_MATCHING_DB.TEST.STEP6_FINAL_COLUMN_MATCHES")

    llm_settings = dbt.config.get("config")["llm_settings"]
    llm_model, llm_temp, llm_top_p, llm_max_tokens = llm_settings['model'], llm_settings['temperature'], llm_settings['top_p'], llm_settings['max_tokens']

    FT = dbt.config.get("config")["feature_thresholds"]
    overlap_threshold, top_n = FT['overlap_threshold'], FT['embedding_top_n']

    embed_model = dbt.config.get("config")["embedding_settings"]["model"]

    # Since we have to return a table, we do these three steps (Steps 7, 8, 9) together
    dfs = translate_nonenglish_entries(session, dfs, final_matches, llm_model, overlap_threshold)
    dfs = apply_gpt_column_transforms(session, dfs, final_matches, llm_model, llm_temp, llm_top_p, llm_max_tokens, overlap_threshold)
    matches = get_embed_match_rows(dbt, session, dfs, final_matches, overlap_threshold, top_n, embed_model)
    return matches
    