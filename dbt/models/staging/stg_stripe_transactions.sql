WITH source AS (
    SELECT * FROM {{ source('raw', 'stripe_transactions') }}
),

renamed AS (
    SELECT
        transaction_id,
        PARSE_DATETIME('%Y-%m-%d', date) as transaction_date,
        customer_id,
        location_id,
        amount as gross_amount,
        fee,
        (amount - fee) as net_amount,
        'stripe' as source_system,
        _PARTITIONTIME as ingestion_timestamp
    FROM source
)

SELECT * FROM renamed 