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

-- setup completion note
SELECT 'autodesk_account_matching_db setup is now complete' AS note;