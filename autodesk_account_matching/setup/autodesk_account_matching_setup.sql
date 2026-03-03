USE ROLE accountadmin;

CREATE OR REPLACE WAREHOUSE autodesk_account_matching
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'warehouse for autodesk account matching';

USE WAREHOUSE autodesk_account_matching;

CREATE DATABASE IF NOT EXISTS autodesk_account_matching_db;
CREATE OR REPLACE SCHEMA autodesk_account_matching_db.raw;
CREATE OR REPLACE SCHEMA autodesk_account_matching_db.dev;
CREATE OR REPLACE SCHEMA autodesk_account_matching_db.prod;


ALTER SCHEMA autodesk_account_matching_db.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA autodesk_account_matching_db.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA autodesk_account_matching_db.dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA autodesk_account_matching_db.prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA autodesk_account_matching_db.prod SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA autodesk_account_matching_db.prod SET METRIC_LEVEL = 'ALL';

CREATE OR REPLACE API INTEGRATION git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;

-- TODO: Add more integrations as needed

-- copy tables from the old database
DROP TABLE IF EXISTS autodesk_account_matching_db.raw.ENR_250;

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.ENR_250
LIKE AUTODESK_ACCOUNT_MATCHING.PUBLIC.ENR_250;

INSERT INTO autodesk_account_matching_db.raw.ENR_250
SELECT * FROM AUTODESK_ACCOUNT_MATCHING.PUBLIC.ENR_250;

DROP TABLE IF EXISTS autodesk_account_matching_db.raw.ENR_400;

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.ENR_400
LIKE AUTODESK_ACCOUNT_MATCHING.PUBLIC.ENR_400;

INSERT INTO autodesk_account_matching_db.raw.ENR_400
SELECT * FROM AUTODESK_ACCOUNT_MATCHING.PUBLIC.ENR_400;

DROP TABLE IF EXISTS autodesk_account_matching_db.raw.ENR_600;

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.ENR_600
LIKE AUTODESK_ACCOUNT_MATCHING.PUBLIC.ENR_600;

INSERT INTO autodesk_account_matching_db.raw.ENR_600
SELECT * FROM AUTODESK_ACCOUNT_MATCHING.PUBLIC.ENR_600;

DROP TABLE IF EXISTS autodesk_account_matching_db.raw.GLOBAL_DATA;

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.GLOBAL_DATA
LIKE AUTODESK_ACCOUNT_MATCHING.PUBLIC.GLOBAL_DATA;

INSERT INTO autodesk_account_matching_db.raw.GLOBAL_DATA
SELECT * FROM AUTODESK_ACCOUNT_MATCHING.PUBLIC.GLOBAL_DATA;

DROP TABLE IF EXISTS autodesk_account_matching_db.raw.MASTER;

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.MASTER
LIKE AUTODESK_ACCOUNT_MATCHING.PUBLIC.MASTER;

INSERT INTO autodesk_account_matching_db.raw.MASTER
SELECT * FROM AUTODESK_ACCOUNT_MATCHING.PUBLIC.MASTER;

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP7_TRANSLATED_DFS_ENR_250 (
    ENR250_RANKING_YEAR TEXT,
    ENR250_RANK TEXT,
    STD_COMPANY_NAME TEXT,
    ADDRESS TEXT,
    CITY TEXT,
    STATE TEXT,
    REGION TEXT,
    POSTAL_CODE TEXT,
    TELEPHONE_NUMBER TEXT,
    FAX_NUMBER TEXT,
    COMPANY_WEBSITE TEXT,
    CEO_NAME TEXT,
    CEO_TITLE TEXT,
    COO_NAME TEXT,
    COO_TITLE TEXT,
    CFO_NAME TEXT,
    CFO_TITLE TEXT,
    COMPANY_NAME_SOUNDEX TEXT,
    WEBSITE_NAME_SOUNDEX TEXT,
    COUNTRY TEXT,
    OLD_COMPANY_NAME TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP7_TRANSLATED_DFS_ENR_400 (
    ENR400_RANKING_YEAR TEXT,
    ENR400_RANK TEXT,
    OLD_COMPANY_NAME TEXT,
    ADDRESS TEXT,
    CITY TEXT,
    STATE TEXT,
    POSTAL_CODE TEXT,
    HQ_PHONE TEXT,
    FAX_NUMBER TEXT,
    COMPANY_WEBSITE TEXT,
    DOM_GC__TOTAL TEXT,
    CEO_NAME TEXT,
    CEO_TITLE TEXT,
    BUSINESS_DEV_OFFICER_NAME TEXT,
    BUSINESS_DEV_OFFICER_TITLE TEXT,
    PUBLIC_RELATIONS_OFFICER_NAME TEXT,
    PUBLIC_RELATIONS_OFFICER_TITLE TEXT,
    HUMAN_RESOURCES_OFFICER_NAME TEXT,
    HUMAN_RESOURCES_OFFICER_TITLE TEXT,
    COMPANY_NAME_SOUNDEX TEXT,
    WEBSITE_NAME_SOUNDEX TEXT,
    COMPANY_NAME TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP7_TRANSLATED_DFS_ENR_600 (
    ENR600_RANKING_YEAR TEXT,
    ENR600_RANK TEXT,
    OLD_COMPANY_NAME TEXT,
    ADDRESS TEXT,
    STATE TEXT,
    POSTAL_CODE TEXT,
    HQ_PHONE TEXT,
    FAX_NUMBER TEXT,
    COMPANY_WEBSITE TEXT,
    REVENUE_TOTAL TEXT,
    NEW_CONTRACTS TEXT,
    FIRM_TYPE TEXT,
    CEO_NAME TEXT,
    CEO_TITLE TEXT,
    BUSINESS_DEV_OFFICER_NAME TEXT,
    BUSINESS_DEV_OFFICER_TITLE TEXT,
    PUBLIC_RELATIONS_OFFICER_NAME TEXT,
    PUBLIC_RELATIONS_OFFICER_TITLE TEXT,
    HUMAN_RESOURCES_OFFICER_NAME TEXT,
    HUMAN_RESOURCES_OFFICER_TITLE TEXT,
    EQUIPMENT_MANAGER_NAME TEXT,
    EQUIPMENT_MANAGER_TITLE TEXT,
    COMPANY_NAME_SOUNDEX TEXT,
    WEBSITE_NAME_SOUNDEX TEXT,
    CITY TEXT,
    COMPANY_NAME TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP7_TRANSLATED_DFS_GLOBAL_DATA (
    OVERVIEW TEXT,
    ENTITY_TYPE TEXT,
    COMPANY_TYPE TEXT,
    NUMBER_OF_EMPLOYEES NUMBER,
    ANNUAL_REVENUE NUMBER,
    REVENUE_YEAR NUMBER,
    EMAIL_ADDRESS TEXT,
    COMPANY_URL TEXT,
    HEADQUARTERS_PHONE_NUMBER TEXT,
    HEADQUARTERS_FAX TEXT,
    HEADQUARTERS_ADDRESS TEXT,
    HEADQUARTERS_STATE TEXT,
    HEADQUARTERS_ZIPCODE TEXT,
    FISCAL_YEAR_END TEXT,
    INTERNATIONAL_SECURITIES_IDENTIFICATION_NUMBER TEXT,
    TICK_SYMBOL TEXT,
    PARENT_COMPANY_ID NUMBER,
    PARENT_COMPANY_NAME TEXT,
    COMPANY_ID VARIANT,
    HEADQUARTERS_CITY TEXT,
    HEADQUARTERS_COUNTRY TEXT,
    DUNS TEXT,
    PRIMARY_INDUSTRY TEXT,
    COMPANY_NAME TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP7_TRANSLATED_DFS_MASTER (
    ID TEXT,
    PARENT_ACCOUNT_CSN__C TEXT,
    INDUSTRY_SUB_SEGMENT__C TEXT,
    GEO__C TEXT,
    SEC_ACCOUNT_NAME__C TEXT,
    COUNTRY_PICKLIST__C TEXT,
    BILLINGCOUNTRY TEXT,
    SIC_CODE_VALUE__C TEXT,
    SEC_ADDRESS1__C TEXT,
    SEC_ADDRESS2__C TEXT,
    SEC_ADDRESS3__C TEXT,
    ADDRESS2__C TEXT,
    ADDRESS1__C TEXT,
    COUNTRY__C TEXT,
    PARENT_ACCOUNT_NAME__C TEXT,
    CITY__C TEXT,
    NAME TEXT,
    ACCOUNT_CSN__C TEXT,
    DUNS_NUMBER__C TEXT,
    INDUSTRY_SEGMENT__C TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP8_TRANSFORMED_DFS_ENR_250 (
    ENR250_RANKING_YEAR TEXT,
    ENR250_RANK TEXT,
    STD_COMPANY_NAME TEXT,
    ADDRESS TEXT,
    CITY TEXT,
    STATE TEXT,
    REGION TEXT,
    POSTAL_CODE TEXT,
    TELEPHONE_NUMBER TEXT,
    FAX_NUMBER TEXT,
    COMPANY_WEBSITE TEXT,
    CEO_NAME TEXT,
    CEO_TITLE TEXT,
    COO_NAME TEXT,
    COO_TITLE TEXT,
    CFO_NAME TEXT,
    CFO_TITLE TEXT,
    COMPANY_NAME_SOUNDEX TEXT,
    WEBSITE_NAME_SOUNDEX TEXT,
    COUNTRY TEXT,
    OLD_COMPANY_NAME TEXT,
    COUNTRY__TRANSFORMED TEXT,
    OLD_COMPANY_NAME__TRANSFORMED TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP8_TRANSFORMED_DFS_ENR_400 (
    ENR400_RANKING_YEAR TEXT,
    ENR400_RANK TEXT,
    OLD_COMPANY_NAME TEXT,
    ADDRESS TEXT,
    CITY TEXT,
    STATE TEXT,
    POSTAL_CODE TEXT,
    HQ_PHONE TEXT,
    FAX_NUMBER TEXT,
    COMPANY_WEBSITE TEXT,
    DOM_GC__TOTAL TEXT,
    CEO_NAME TEXT,
    CEO_TITLE TEXT,
    BUSINESS_DEV_OFFICER_NAME TEXT,
    BUSINESS_DEV_OFFICER_TITLE TEXT,
    PUBLIC_RELATIONS_OFFICER_NAME TEXT,
    PUBLIC_RELATIONS_OFFICER_TITLE TEXT,
    HUMAN_RESOURCES_OFFICER_NAME TEXT,
    HUMAN_RESOURCES_OFFICER_TITLE TEXT,
    COMPANY_NAME_SOUNDEX TEXT,
    WEBSITE_NAME_SOUNDEX TEXT,
    COMPANY_NAME TEXT,
    COMPANY_NAME__TRANSFORMED TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP8_TRANSFORMED_DFS_ENR_600 (
    ENR600_RANKING_YEAR TEXT,
    ENR600_RANK TEXT,
    OLD_COMPANY_NAME TEXT,
    ADDRESS TEXT,
    STATE TEXT,
    POSTAL_CODE TEXT,
    HQ_PHONE TEXT,
    FAX_NUMBER TEXT,
    COMPANY_WEBSITE TEXT,
    REVENUE_TOTAL TEXT,
    NEW_CONTRACTS TEXT,
    FIRM_TYPE TEXT,
    CEO_NAME TEXT,
    CEO_TITLE TEXT,
    BUSINESS_DEV_OFFICER_NAME TEXT,
    BUSINESS_DEV_OFFICER_TITLE TEXT,
    PUBLIC_RELATIONS_OFFICER_NAME TEXT,
    PUBLIC_RELATIONS_OFFICER_TITLE TEXT,
    HUMAN_RESOURCES_OFFICER_NAME TEXT,
    HUMAN_RESOURCES_OFFICER_TITLE TEXT,
    EQUIPMENT_MANAGER_NAME TEXT,
    EQUIPMENT_MANAGER_TITLE TEXT,
    COMPANY_NAME_SOUNDEX TEXT,
    WEBSITE_NAME_SOUNDEX TEXT,
    CITY TEXT,
    COMPANY_NAME TEXT,
    CITY__TRANSFORMED TEXT,
    COMPANY_NAME__TRANSFORMED TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP8_TRANSFORMED_DFS_GLOBAL_DATA (
    OVERVIEW TEXT,
    ENTITY_TYPE TEXT,
    COMPANY_TYPE TEXT,
    NUMBER_OF_EMPLOYEES NUMBER,
    ANNUAL_REVENUE NUMBER,
    REVENUE_YEAR NUMBER,
    EMAIL_ADDRESS TEXT,
    COMPANY_URL TEXT,
    HEADQUARTERS_PHONE_NUMBER TEXT,
    HEADQUARTERS_FAX TEXT,
    HEADQUARTERS_ADDRESS TEXT,
    HEADQUARTERS_STATE TEXT,
    HEADQUARTERS_ZIPCODE TEXT,
    FISCAL_YEAR_END TEXT,
    INTERNATIONAL_SECURITIES_IDENTIFICATION_NUMBER TEXT,
    TICK_SYMBOL TEXT,
    PARENT_COMPANY_ID NUMBER,
    PARENT_COMPANY_NAME TEXT,
    COMPANY_ID VARIANT,
    HEADQUARTERS_CITY TEXT,
    HEADQUARTERS_COUNTRY TEXT,
    DUNS TEXT,
    PRIMARY_INDUSTRY TEXT,
    COMPANY_NAME TEXT,
    COMPANY_ID__TRANSFORMED TEXT,
    HEADQUARTERS_CITY__TRANSFORMED TEXT,
    HEADQUARTERS_COUNTRY__TRANSFORMED TEXT,
    DUNS__TRANSFORMED TEXT,
    PRIMARY_INDUSTRY__TRANSFORMED TEXT,
    COMPANY_NAME__TRANSFORMED TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.STEP8_TRANSFORMED_DFS_MASTER (
    ID TEXT,
    PARENT_ACCOUNT_CSN__C TEXT,
    INDUSTRY_SUB_SEGMENT__C TEXT,
    GEO__C TEXT,
    SEC_ACCOUNT_NAME__C TEXT,
    COUNTRY_PICKLIST__C TEXT,
    BILLINGCOUNTRY TEXT,
    SIC_CODE_VALUE__C TEXT,
    SEC_ADDRESS1__C TEXT,
    SEC_ADDRESS2__C TEXT,
    SEC_ADDRESS3__C TEXT,
    ADDRESS2__C TEXT,
    ADDRESS1__C TEXT,
    COUNTRY__C TEXT,
    PARENT_ACCOUNT_NAME__C TEXT,
    CITY__C TEXT,
    NAME TEXT,
    ACCOUNT_CSN__C TEXT,
    DUNS_NUMBER__C TEXT,
    INDUSTRY_SEGMENT__C TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step10_final_llm_row_matches (
    ENRICHMENT_ROW_INDEX NUMBER NOT NULL,
    MASTER_ROW_INDEX NUMBER NOT NULL,
    MASTER_DATASET TEXT,
    ENRICHMENT_DATASET TEXT,
    ENRICHMENT_EVIDENCE TEXT,
    MASTER_EVIDENCE TEXT,
    SIMILARITY ARRAY NOT NULL,
    AVG_SIMILARITY FLOAT,
    FINAL_LLM_DECISION TEXT,
    FINAL_LLM_JUSTIFICATION TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step2_column_metadata_descriptions (
    "Dataset" TEXT,
    "Column" TEXT,
    "Detected Data Type" TEXT,
    "Example Values" TEXT,
    "Description" TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step3_all_column_pair_features (
    master_dataset TEXT,
    enrichment_dataset TEXT,
    master_column TEXT,
    enrichment_column TEXT,
    master_description TEXT,
    enrichment_description TEXT,
    master_data_type TEXT,
    enrichment_data_type TEXT,
    master_example_values TEXT,
    enrichment_example_values TEXT,
    type_mismatch BOOLEAN,
    master_fill_rate FLOAT,
    enrichment_fill_rate FLOAT,
    fuzzy_ratio FLOAT,
    fuzzy_token_sort_ratio FLOAT,
    fuzzy_token_set_ratio FLOAT,
    description_embedding_similarity FLOAT,
    overlap_master_ratio FLOAT,
    overlap_enrichment_ratio FLOAT,
    overlap_jaccard FLOAT,
    avg_length_diff FLOAT,
    entropy_master FLOAT,
    entropy_enrichment FLOAT,
    entropy_gap FLOAT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step4_filtered_column_pairs (
    master_dataset TEXT,
    enrichment_dataset TEXT,
    master_column TEXT,
    enrichment_column TEXT,
    master_description TEXT,
    enrichment_description TEXT,
    master_data_type TEXT,
    enrichment_data_type TEXT,
    master_example_values TEXT,
    enrichment_example_values TEXT,
    type_mismatch BOOLEAN,
    master_fill_rate FLOAT,
    enrichment_fill_rate FLOAT,
    fuzzy_ratio FLOAT,
    fuzzy_token_sort_ratio FLOAT,
    fuzzy_token_set_ratio FLOAT,
    description_embedding_similarity FLOAT,
    overlap_master_ratio FLOAT,
    overlap_enrichment_ratio FLOAT,
    overlap_jaccard FLOAT,
    avg_length_diff FLOAT,
    entropy_master FLOAT,
    entropy_enrichment FLOAT,
    entropy_gap FLOAT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step5_gpt_column_pair_classification (
    master_dataset TEXT,
    enrichment_dataset TEXT,
    master_column TEXT,
    enrichment_column TEXT,
    master_description TEXT,
    enrichment_description TEXT,
    master_data_type TEXT,
    enrichment_data_type TEXT,
    master_example_values TEXT,
    enrichment_example_values TEXT,
    type_mismatch BOOLEAN,
    master_fill_rate FLOAT,
    enrichment_fill_rate FLOAT,
    fuzzy_ratio FLOAT,
    fuzzy_token_sort_ratio FLOAT,
    fuzzy_token_set_ratio FLOAT,
    description_embedding_similarity FLOAT,
    overlap_master_ratio FLOAT,
    overlap_enrichment_ratio FLOAT,
    overlap_jaccard FLOAT,
    avg_length_diff FLOAT,
    entropy_master FLOAT,
    entropy_enrichment FLOAT,
    entropy_gap FLOAT,
    status TEXT,
    chatgpt_decision TEXT,
    chatgpt_justification TEXT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step6_final_column_matches (
    master_column TEXT,
    enrichment_dataset TEXT,
    enrichment_column TEXT,
    enrichment_candidates TEXT,
    weighted_score FLOAT,
    percent_master_in_enrichment FLOAT,
    percent_enrichment_in_master FLOAT
);

CREATE TABLE IF NOT EXISTS autodesk_account_matching_db.raw.step9_final_transformed_dfs (
    ENRICHMENT_ROW_INDEX NUMBER NOT NULL,
    MASTER_ROW_INDEX NUMBER NOT NULL,
    MASTER_DATASET TEXT,
    ENRICHMENT_DATASET TEXT,
    ENRICHMENT_EVIDENCE TEXT,
    MASTER_EVIDENCE TEXT,
    SIMILARITY ARRAY NOT NULL,
    AVG_SIMILARITY FLOAT
);

-- create some helper functions
CREATE OR REPLACE FUNCTION AUTODESK_ACCOUNT_MATCHING_DB.RAW.IS_ENGLISH_LANGDETECT_08(text STRING)
RETURNS BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('langdetect')
HANDLER = 'is_english'
AS
$$
import re
from langdetect import detect_langs

_ENGLISH_ASCII_FULLMATCH = r'[A-Za-z0-9\s.,&()\-\'"]+'
THRESHOLD = 0.8

def is_english(text):
    if text is None:
        return True
    if re.fullmatch(_ENGLISH_ASCII_FULLMATCH, text):
        return True
    try:
        langs = detect_langs(text)
        return any(l.lang == "en" and l.prob > THRESHOLD for l in langs)
    except Exception:
        return False
$$;

CREATE OR REPLACE FUNCTION AUTODESK_ACCOUNT_MATCHING_DB.RAW.TOKEN_SET_RATIO(
    S1 STRING,
    S2 STRING
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('rapidfuzz')
HANDLER = 'tsr'
AS
$$
from rapidfuzz.fuzz import token_set_ratio

def tsr(s1, s2):
    # mirror your pandas logic: if either is null, return None
    if s1 is None or s2 is None:
        return None

    # rapidfuzz already handles strings; ensure str for safety
    return float(token_set_ratio(str(s1), str(s2)))
$$;

-- TODO: Embed the concatenations of the key (test all the options); between comparing indiv. vs. concatenated

-- setup completion note
SELECT 'autodesk_account_matching_db setup is now complete' AS note;