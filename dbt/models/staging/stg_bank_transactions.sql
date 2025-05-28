WITH source AS (
    SELECT * FROM {{ source('raw', 'bank_transactions') }}
),

renamed AS (
    SELECT
        transaction_id,
        PARSE_DATETIME('%Y-%m-%d', date) as transaction_date,
        customer_id,
        location_id,
        amount as net_amount,
        'bank' as source_system,
        _PARTITIONTIME as ingestion_timestamp
    FROM source
)

SELECT * FROM renamed 