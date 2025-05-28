"""
Database loaders for BigQuery and Redshift.
"""
import logging
from typing import Optional, Union
import dask.dataframe as dd
from google.cloud import bigquery
from sqlalchemy import create_engine
import redshift_connector
import dask_bigquery

logger = logging.getLogger(__name__)

class BigQueryLoader:
    def __init__(self, project_id: str):
        """Initialize BigQuery client."""
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        
    def load_table(self, query: str) -> dd.DataFrame:
        """
        Load data from BigQuery table using a query.
        Uses dask-bigquery for efficient parallel loading.
        """
        logger.info(f"Loading data from BigQuery with query: {query}")
        try:
            # Use dask_bigquery for efficient parallel loading
            df = dask_bigquery.read_gbq(
                query,
                project_id=self.project_id,
                parse_dates=['date']  # Assuming 'date' column exists
            )
            logger.info("Successfully loaded data from BigQuery")
            return df
        except Exception as e:
            logger.error(f"Failed to load data from BigQuery: {str(e)}")
            raise

class RedshiftLoader:
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5439,
        schema: str = "public"
    ):
        """Initialize Redshift connection."""
        self.connection_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
            "schema": schema
        }
        
    def load_table(self, query: str) -> dd.DataFrame:
        """
        Load data from Redshift table using a query.
        Uses SQLAlchemy for connection and dask for parallel processing.
        """
        logger.info(f"Loading data from Redshift with query: {query}")
        try:
            # Create SQLAlchemy engine
            connection_string = (
                f"postgresql+psycopg2://{self.connection_params['user']}:{self.connection_params['password']}"
                f"@{self.connection_params['host']}:{self.connection_params['port']}"
                f"/{self.connection_params['database']}"
            )
            engine = create_engine(connection_string)
            
            # Use dask to read from SQL in parallel
            df = dd.read_sql_table(
                query,
                engine,
                index_col='id',  # Adjust based on your table structure
                parse_dates=['date']  # Assuming 'date' column exists
            )
            logger.info("Successfully loaded data from Redshift")
            return df
        except Exception as e:
            logger.error(f"Failed to load data from Redshift: {str(e)}")
            raise

def get_loader(source_type: str, **connection_params) -> Optional[Union[BigQueryLoader, RedshiftLoader]]:
    """
    Factory function to get appropriate loader based on source type.
    """
    if source_type == "bigquery":
        return BigQueryLoader(project_id=connection_params["project_id"])
    elif source_type == "redshift":
        return RedshiftLoader(
            host=connection_params["host"],
            database=connection_params["database"],
            user=connection_params["user"],
            password=connection_params["password"],
            port=connection_params.get("port", 5439),
            schema=connection_params.get("schema", "public")
        )
    else:
        raise ValueError(f"Unsupported source type: {source_type}") 