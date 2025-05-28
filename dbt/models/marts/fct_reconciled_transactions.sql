WITH stripe_txns AS (
    SELECT * FROM {{ ref('stg_stripe_transactions') }}
),

bank_txns AS (
    SELECT * FROM {{ ref('stg_bank_transactions') }}
),

-- Match on date, customer_id, location_id, and amount within tolerance
matched AS (
    SELECT 
        s.transaction_id as stripe_transaction_id,
        b.transaction_id as bank_transaction_id,
        s.transaction_date,
        s.customer_id,
        s.location_id,
        s.gross_amount as stripe_gross_amount,
        s.fee as stripe_fee,
        s.net_amount as stripe_net_amount,
        b.net_amount as bank_amount,
        ABS(s.net_amount - b.net_amount) as amount_difference,
        CASE 
            WHEN ABS(s.net_amount - b.net_amount) <= 0.05 THEN 'MATCHED'
            ELSE 'AMOUNT_VARIANCE'
        END as match_status,
        CURRENT_TIMESTAMP() as reconciliation_timestamp
    FROM stripe_txns s
    INNER JOIN bank_txns b
        ON s.customer_id = b.customer_id
        AND s.location_id = b.location_id
        AND DATE(s.transaction_date) = DATE(b.transaction_date)
),

-- Unmatched Stripe transactions
unmatched_stripe AS (
    SELECT 
        s.transaction_id as stripe_transaction_id,
        NULL as bank_transaction_id,
        s.transaction_date,
        s.customer_id,
        s.location_id,
        s.gross_amount as stripe_gross_amount,
        s.fee as stripe_fee,
        s.net_amount as stripe_net_amount,
        NULL as bank_amount,
        NULL as amount_difference,
        'UNMATCHED_STRIPE' as match_status,
        CURRENT_TIMESTAMP() as reconciliation_timestamp
    FROM stripe_txns s
    LEFT JOIN matched m
        ON s.transaction_id = m.stripe_transaction_id
    WHERE m.stripe_transaction_id IS NULL
),

-- Unmatched bank transactions
unmatched_bank AS (
    SELECT 
        NULL as stripe_transaction_id,
        b.transaction_id as bank_transaction_id,
        b.transaction_date,
        b.customer_id,
        b.location_id,
        NULL as stripe_gross_amount,
        NULL as stripe_fee,
        NULL as stripe_net_amount,
        b.net_amount as bank_amount,
        NULL as amount_difference,
        'UNMATCHED_BANK' as match_status,
        CURRENT_TIMESTAMP() as reconciliation_timestamp
    FROM bank_txns b
    LEFT JOIN matched m
        ON b.transaction_id = m.bank_transaction_id
    WHERE m.bank_transaction_id IS NULL
)

-- Combine all results
SELECT * FROM matched
UNION ALL
SELECT * FROM unmatched_stripe
UNION ALL
SELECT * FROM unmatched_bank 