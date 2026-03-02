select *
from {{ source('raw', 'MASTER') }}
