select *
from {{ source('raw', 'STEP7_TRANSLATED_DFS_GLOBAL_DATA') }}
