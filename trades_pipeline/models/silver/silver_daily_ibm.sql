{{ config(materialized='table') }}

select
    date,
    open,
    high,
    low,
    close,
    volume,
    close - open as daily_change,
    round((close - open)/open*100,2) as daily_change_pct
from {{ ref('stg_daily_ibm') }}
where volume > 0