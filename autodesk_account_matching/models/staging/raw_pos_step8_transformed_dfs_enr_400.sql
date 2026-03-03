select *
from {{ source('raw', 'STEP8_TRANSFORMED_DFS_ENR_400') }}
