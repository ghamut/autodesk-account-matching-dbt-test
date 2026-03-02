select *
from {{ source('raw', 'GLOBAL_DATA') }}
