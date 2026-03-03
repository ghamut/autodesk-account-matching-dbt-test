select *
from {{ source('raw', 'step5_gpt_column_pair_classification') }}
