select *
from {{ source('raw', 'step9_final_transformed_dfs') }}
