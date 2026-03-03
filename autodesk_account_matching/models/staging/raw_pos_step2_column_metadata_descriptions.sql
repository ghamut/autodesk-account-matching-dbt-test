select *
from {{ source('raw', 'step2_column_metadata_descriptions') }}
