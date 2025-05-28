"""
Source loader module for handling different data sources.
"""
import logging
import os
from typing import Dict, Any
import dask.dataframe as dd
from urllib.parse import urlparse
from .db_loader import get_loader

logger = logging.getLogger(__name__)

class SourceLoader:
    """Handles loading data from various sources (files, databases)."""
    
    @staticmethod
    def load_source(source_config: Dict[str, Any]) -> dd.DataFrame:
        """
        Load data from a configured source.
        
        Args:
            source_config: Source configuration dictionary containing:
                - type: "file|bigquery|redshift"
                - path: Path for file sources
                - query: SQL for database sources
                - connection: Database connection parameters
        
        Returns:
            Dask DataFrame containing the loaded data
        """
        source_type = source_config["type"]
        logger.info(f"Loading data from {source_type} source")
        
        if source_type == "file":
            return SourceLoader._load_file(source_config["path"])
        else:
            return SourceLoader._load_database(source_config)
    
    @staticmethod
    def _load_file(path: str) -> dd.DataFrame:
        """Load data from a file source (local, S3, or GCS)."""
        try:
            parsed_url = urlparse(path)
            separator = '|' if path.endswith('.psv') else ','
            
            logger.info(f"Attempting to read file from: {path}")
            
            # Validate file existence based on source type
            if parsed_url.scheme == 's3':
                import s3fs
                fs = s3fs.S3FileSystem()
                if not fs.exists(path):
                    raise FileNotFoundError(f"File not found in S3: {path}")
                    
            elif parsed_url.scheme == 'gs':
                import gcsfs
                fs = gcsfs.GCSFileSystem()
                if not fs.exists(path):
                    raise FileNotFoundError(f"File not found in GCS: {path}")
                    
            elif parsed_url.scheme == '':  # Local file
                if not os.path.exists(path):
                    raise FileNotFoundError(f"Local file not found: {path}")

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
    
    @staticmethod
    def _load_database(source_config: Dict[str, Any]) -> dd.DataFrame:
        """Load data from a database source (BigQuery or Redshift)."""
        try:
            loader = get_loader(source_config["type"], **source_config["connection"])
            return loader.load_table(source_config["query"])
        except Exception as e:
            logger.error(f"Failed to load from {source_config['type']}: {str(e)}")
            raise 