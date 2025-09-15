select *
from {{ ref('stg_daily_ibm') }}
where open > 500