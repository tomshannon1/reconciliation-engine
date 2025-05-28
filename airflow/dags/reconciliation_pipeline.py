"""
DAG for transaction reconciliation pipeline.
"""
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'transaction_reconciliation',
    default_args=default_args,
    description='Pipeline for reconciling Stripe and bank transactions',
    schedule_interval='0 5 * * *',  # Run daily at 5 AM
    catchup=False
)

# Task 1: Load Stripe transactions from GCS to BigQuery
load_stripe_transactions = GCSToBigQueryOperator(
    task_id='load_stripe_transactions',
    bucket='your-gcs-bucket',
    source_objects=['stripe/daily/*.csv'],
    destination_project_dataset_table='raw.stripe_transactions',
    schema_fields=[
        {'name': 'transaction_id', 'type': 'STRING'},
        {'name': 'date', 'type': 'STRING'},
        {'name': 'customer_id', 'type': 'STRING'},
        {'name': 'location_id', 'type': 'STRING'},
        {'name': 'amount', 'type': 'FLOAT'},
        {'name': 'fee', 'type': 'FLOAT'}
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

# Task 2: Load bank transactions from GCS to BigQuery
load_bank_transactions = GCSToBigQueryOperator(
    task_id='load_bank_transactions',
    bucket='your-gcs-bucket',
    source_objects=['bank/daily/*.csv'],
    destination_project_dataset_table='raw.bank_transactions',
    schema_fields=[
        {'name': 'transaction_id', 'type': 'STRING'},
        {'name': 'date', 'type': 'STRING'},
        {'name': 'customer_id', 'type': 'STRING'},
        {'name': 'location_id', 'type': 'STRING'},
        {'name': 'amount', 'type': 'FLOAT'}
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

# Task 3: Run dbt models
def run_dbt_models():
    """Execute dbt models for transaction reconciliation."""
    import subprocess
    subprocess.run(['dbt', 'run', '--models', 'marts.fct_reconciled_transactions'], check=True)

dbt_reconciliation = PythonOperator(
    task_id='dbt_reconciliation',
    python_callable=run_dbt_models,
    dag=dag
)

# Task 4: Create partitioned table for reconciliation results
create_partitioned_results = BigQueryOperator(
    task_id='create_partitioned_results',
    sql="""
    CREATE OR REPLACE TABLE `marts.reconciliation_results`
    PARTITION BY DATE(reconciliation_timestamp)
    AS SELECT * FROM `marts.fct_reconciled_transactions`
    """,
    use_legacy_sql=False,
    dag=dag
)

# Define task dependencies
[load_stripe_transactions, load_bank_transactions] >> dbt_reconciliation >> create_partitioned_results 