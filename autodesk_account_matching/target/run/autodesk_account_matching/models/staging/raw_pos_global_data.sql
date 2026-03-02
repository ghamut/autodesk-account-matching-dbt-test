
  create or replace   view autodesk_account_matching_db.dev.raw_pos_global_data
  
   as (
    select *
from AUTODESK_ACCOUNT_MATCHING_DB.RAW.GLOBAL_DATA
  );

