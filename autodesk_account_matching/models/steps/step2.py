from openai import OpenAI, RateLimitError, APIError, Timeout

client = OpenAI(api_key=...)

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
        qcol = _quote_ident(col)

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

def describe_dataset(dataset, meta_data, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model= "gpt-4o-2024-08-06",
                max_tokens=1000,
                temperature=0,
                seed=43,
                messages=[
                    {"role": "system", "content": "Do not use code to answer the question. Provide only code as your response, no explanations. Do not use markdown formatting."},
                    {"role": "user", "content": f"""Forget all previous instructions.
You will be provided with metadata for a set of columns in a dataset.
Return a dictionary with one key for each column in the dataset. 
For each column, provide a brief description given the Column Name, Example Values, and your best judgement.
Prioritize the Example Values when generating the description, and use the Column Name as a secondary reference.
example output: {{"<column1_name>": "<description1>", "<column2_name>" : "<description2>" }}
The metadata follows:
```json
{json.dumps(meta_data[dataset])}
```"""}
                ]
            )
            return dataset, json.loads(response.choices[0].message.content.strip())
        except (RateLimitError, Timeout, APIError, ValueError) as e:
            time.sleep(2 ** attempt + random.uniform(0, 1))
    return dataset, {}

def generate_metadata(session, dfs):
    print("------------------------------------------------------------")
    print("Step 2: Generating column metadata and descriptions...")
    print("------------------------------------------------------------")
    meta_data = {name: generate_column_stats(df) for name, df in dfs.items()}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(describe_dataset, dataset, meta_data): dataset
            for dataset in meta_data
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Describing columns"):
            dataset, descs = future.result()
            for col in meta_data[dataset]:
                meta_data[dataset][col]['Description'] = descs.get(col, "No description provided by ChatGPT.")

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
        table_name="step2_column_metadata_descriptions",
        schema="RAW",
        overwrite=True
    )
    print(f"✔ Metadata and descriptions written to Snowflake\n")
    return session.table("AUTODESK_ACCOUNT_MATCHING_DB.RAW.\"step2_column_metadata_descriptions\"")

def model(dbt, session):
    dfs = load_data(dbt)
    meta_data = generate_metadata(session, dfs)
    return meta_data
    