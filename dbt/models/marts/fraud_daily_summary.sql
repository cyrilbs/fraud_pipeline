select
    event_date,
    decision,
    count(*) as transaction_count,
    avg(fraud_probability) as avg_fraud_probability
from {{ ref('stg_fraud_decisions') }}
group by 1, 2
order by 1, 2
