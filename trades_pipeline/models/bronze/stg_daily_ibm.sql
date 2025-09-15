{{ config(materialized='table') }}

select
    date,
    open,
    high,
    low,
    close,
    volume
from {{ source('stat', 'daily_ibm') }}