select *
from {{ source('raw', 'step10_final_llm_row_matches') }}
