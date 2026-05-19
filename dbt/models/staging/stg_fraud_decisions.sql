with raw as (
    select *
    from read_parquet(
        '/data/parquet/fraud_decisions/**/*.parquet',
        hive_partitioning := true
    )
)

select
    transaction_id,
    cast(fraud_probability as double) as fraud_probability,
    decision,
    cast(timestamp as timestamp) as event_timestamp,
    batch_id,
    event_date
from raw
