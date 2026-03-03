select *
from {{ source('raw', 'step6_final_column_matches') }}
