"""
Local pipeline runner for testing the reconciliation process.
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from generate_test_data import generate_test_data
from dotenv import load_dotenv
import subprocess
import json

def setup_dbt_profile():
    """Create a dbt profile for local development."""
    home_dir = os.path.expanduser("~")
    dbt_dir = os.path.join(home_dir, ".dbt")
    os.makedirs(dbt_dir, exist_ok=True)
    
    profile = {
        "reconciliation_engine": {
            "outputs": {
                "dev": {
                    "type": "bigquery",
                    "method": "oauth",
                    "project": os.getenv("GOOGLE_CLOUD_PROJECT"),
                    "dataset": "reconciliation_dev",
                    "threads": 4,
                    "timeout_seconds": 300,
                    "location": "US"
                }
            },
            "target": "dev"
        }
    }
    
    with open(os.path.join(dbt_dir, "profiles.yml"), "w") as f:
        json.dump(profile, f, indent=2)

def run_local_pipeline():
    """Run the reconciliation pipeline locally."""
    print("Starting local reconciliation pipeline...")
    
    # 1. Generate test data
    print("\n1. Generating test data...")
    data_dir = "data/raw"
    os.makedirs(data_dir, exist_ok=True)
    
    internal_df, external_df = generate_test_data(
        num_records=1000,
        match_rate=0.85,
        amount_variance_rate=0.10,
        date_variance_rate=0.05,
        id_variance_rate=0.05,
        output_dir=data_dir
    )
    
    print(f"Generated {len(internal_df)} internal records")
    print(f"Generated {len(external_df)} external records")
    
    # 2. Load data to BigQuery (if GCP credentials are set up)
    if os.getenv("GOOGLE_CLOUD_PROJECT"):
        print("\n2. Loading data to BigQuery...")
        from google.cloud import bigquery
        
        client = bigquery.Client()
        dataset_id = f"{os.getenv('GOOGLE_CLOUD_PROJECT')}.raw"
        
        # Create dataset if it doesn't exist
        try:
            client.get_dataset(dataset_id)
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            client.create_dataset(dataset, exists_ok=True)
        
        # Load data to BigQuery
        for df, table_name in [(internal_df, "stripe_transactions"), 
                             (external_df, "bank_transactions")]:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
            table_id = f"{dataset_id}.{table_name}"
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print(f"Loaded {len(df)} rows to {table_id}")
        
        # 3. Run dbt models
        print("\n3. Running dbt models...")
        try:
            setup_dbt_profile()
            subprocess.run(["dbt", "run"], check=True)
            subprocess.run(["dbt", "test"], check=True)
            
            # 4. Query and display results
            print("\n4. Reconciliation Results:")
            query = """
            SELECT 
                match_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM `reconciliation_dev.fct_reconciled_transactions`
            GROUP BY match_status
            ORDER BY count DESC
            """
            df_results = client.query(query).to_dataframe()
            print("\nReconciliation Summary:")
            print(df_results.to_string(index=False))
            
        except subprocess.CalledProcessError as e:
            print(f"Error running dbt: {e}")
    else:
        print("\nSkipping BigQuery upload and dbt steps - no GCP credentials found")
        print("To run the full pipeline, set GOOGLE_CLOUD_PROJECT environment variable")
        
        # Show local data summary instead
        print("\nLocal Data Summary:")
        print("\nInternal Transactions:")
        print(internal_df.head())
        print("\nExternal Transactions:")
        print(external_df.head())

if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file
    run_local_pipeline() 