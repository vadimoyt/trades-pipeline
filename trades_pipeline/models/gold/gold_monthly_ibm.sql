{{ config(materialized='table') }}

select
    date_trunc('month', date) as month,
    avg(open) as avg_open,
    avg(close) as avg_close,
    sum(volume) as total_volume,
    avg(daily_change_pct) as avg_daily_change_pct
from {{ ref('silver_daily_ibm') }}
group by 1
order by 1
