import argparse
import yaml
import logging
import dask.dataframe as dd
import os
from urllib.parse import urlparse
from botocore.exceptions import ClientError, NoCredentialsError
from google.auth.exceptions import DefaultCredentialsError
from src.recon_engine import ReconciliationEngine
from src.db_loader import get_loader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def load_source(source_config):
    """
    Load data from either a file or database source based on configuration.
    
    source_config format:
    {
        "type": "file|bigquery|redshift",
        "path": "path/to/file.csv",  # for file type
        "query": "SELECT * FROM table",  # for database types
        "connection": {  # for database types
            "project_id": "...",  # for BigQuery
            "host": "...",        # for Redshift
            "database": "...",    # for Redshift
            "user": "...",        # for Redshift
            "password": "...",    # for Redshift
            "port": 5439,         # optional for Redshift
            "schema": "public"    # optional for Redshift
        }
    }
    """
    if source_config["type"] == "file":
        return load_csv(source_config["path"])
    else:
        loader = get_loader(source_config["type"], **source_config["connection"])
        return loader.load_table(source_config["query"])

def load_csv(path):
    """
    Load CSV/PSV file from local filesystem, S3, or GCS.
    Supports paths in formats:
    - Local: path/to/file.csv
    - S3: s3://bucket/path/to/file.csv
    - GCS: gs://bucket/path/to/file.csv
    """
    try:
        parsed_url = urlparse(path)
        separator = '|' if path.endswith('.psv') else ','
        
        # Log the attempt to read the file
        logger.info(f"Attempting to read file from: {path}")
        
        if parsed_url.scheme == 's3':
            try:
                import s3fs
                # Test S3 credentials before attempting to read
                fs = s3fs.S3FileSystem()
                if not fs.exists(path):
                    raise FileNotFoundError(f"File not found in S3: {path}")
            except (ImportError, NoCredentialsError) as e:
                logger.error(f"S3 authentication failed. Please ensure AWS credentials are properly configured: {str(e)}")
                raise
            except ClientError as e:
                logger.error(f"AWS S3 error: {str(e)}")
                raise
                
        elif parsed_url.scheme == 'gs':
            try:
                import gcsfs
                # Test GCS credentials before attempting to read
                fs = gcsfs.GCSFileSystem()
                if not fs.exists(path):
                    raise FileNotFoundError(f"File not found in GCS: {path}")
            except ImportError:
                logger.error("gcsfs not installed. Please install it for GCS support.")
                raise
            except DefaultCredentialsError as e:
                logger.error(f"GCS authentication failed. Please ensure GOOGLE_APPLICATION_CREDENTIALS is properly set: {str(e)}")
                raise
                
        elif parsed_url.scheme == '':  # Local file
            if not os.path.exists(path):
                logger.error(f"Local file not found: {path}")
                raise FileNotFoundError(f"File not found: {path}")

        # Attempt to read the file
        df = dd.read_csv(
            path,
            sep=separator,
            parse_dates=['date']
        )
        
        logger.info(f"Successfully loaded file: {path}")
        return df

    except Exception as e:
        logger.error(f"Failed to load file {path}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Run reconciliation between internal and external sources (files or databases).")
    parser.add_argument("--config", type=str, default="examples/config.yaml", help="Path to config YAML file")
    
    args = parser.parse_args()
    config = load_config(args.config)

    # Load data from sources specified in config
    internal_df = load_source(config["sources"]["internal"])
    external_df = load_source(config["sources"]["external"])

    engine = ReconciliationEngine(
        match_on=config["matching"]["match_on"],
        internal_key=config["matching"]["internal_key"],
        external_key=config["matching"]["external_key"],
        amount_tolerance=config.get("tolerances", {}).get("amount", 0.0),
        date_tolerance_days=config.get("tolerances", {}).get("days", 0)
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(internal_df, external_df)

    output_path = config["output"]["path"]
    matched.compute().to_csv(f"{output_path}/{config['output']['reconciled_file']}", index=False)
    unmatched_internal.compute().to_csv(f"{output_path}/{config['output']['unmatched_internal_file']}", index=False)
    unmatched_external.compute().to_csv(f"{output_path}/{config['output']['unmatched_external_file']}", index=False)

    print(f"[✅] Outputs written to `{output_path}`")

if __name__ == "__main__":
    main()
