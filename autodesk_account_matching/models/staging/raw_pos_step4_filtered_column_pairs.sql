select *
from {{ source('raw', 'step4_filtered_column_pairs') }}
