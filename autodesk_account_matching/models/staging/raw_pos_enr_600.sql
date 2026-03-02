select *
from {{ source('raw', 'ENR_600') }}
