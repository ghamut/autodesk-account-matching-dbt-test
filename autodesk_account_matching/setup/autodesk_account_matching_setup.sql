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

-- setup completion note
SELECT 'autodesk_account_matching_db setup is now complete' AS note;