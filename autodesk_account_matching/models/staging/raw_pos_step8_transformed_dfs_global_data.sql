select *
from {{ source('raw', 'STEP8_TRANSFORMED_DFS_GLOBAL_DATA') }}
