select *
from {{ source('raw', 'step3_all_column_pair_features') }}
