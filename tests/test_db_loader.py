import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from src.db_loader import BigQueryLoader, RedshiftLoader, get_loader
from google.cloud import bigquery
from sqlalchemy import create_engine

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client', autospec=True) as mock:
        # Create a mock instance
        mock_instance = mock.return_value
        mock_instance.project = "test-project"
        yield mock

@pytest.fixture
def mock_dask_bigquery():
    with patch('dask_bigquery.read_gbq') as mock:
        # Create a mock DataFrame
        mock_df = dd.from_pandas(
            pd.DataFrame({
                'date': [datetime.now()],
                'amount': [100.0],
                'customer_id': ['1001'],
                'location_id': ['501']
            }),
            npartitions=1
        )
        mock.return_value = mock_df
        yield mock

@pytest.fixture
def mock_sqlalchemy_engine():
    with patch('sqlalchemy.create_engine') as mock:
        yield mock

def test_bigquery_loader_init(mock_bigquery_client):
    """Test BigQuery loader initialization."""
    with patch('google.auth.default', return_value=(MagicMock(), "test-project")):
        loader = BigQueryLoader(project_id="test-project")
        mock_bigquery_client.assert_called_once_with(project="test-project")

def test_bigquery_loader_load_table(mock_bigquery_client, mock_dask_bigquery):
    """Test BigQuery data loading."""
    with patch('google.auth.default', return_value=(MagicMock(), "test-project")):
        loader = BigQueryLoader(project_id="test-project")
        result = loader.load_table("SELECT * FROM table")

        # Verify the mock was called with the correct arguments
        mock_dask_bigquery.assert_called_once()
        call_args = mock_dask_bigquery.call_args[0]
        call_kwargs = mock_dask_bigquery.call_args[1]
        assert call_args[0] == "SELECT * FROM table"  # First positional arg should be the query
        assert call_kwargs.get('parse_dates') == ['date']
        assert call_kwargs.get('project_id') == "test-project"
        assert isinstance(result, dd.DataFrame)

def test_redshift_loader_init():
    """Test Redshift loader initialization."""
    loader = RedshiftLoader(
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-pass"
    )
    assert loader.connection_params["host"] == "test-host"
    assert loader.connection_params["database"] == "test-db"
    assert loader.connection_params["port"] == 5439  # default port

def test_redshift_loader_load_table(mock_sqlalchemy_engine):
    """Test Redshift data loading."""
    with patch('dask.dataframe.read_sql_table') as mock_read_sql:
        # Setup mock return value
        mock_df = dd.from_pandas(
            pd.DataFrame({
                'date': [datetime.now()],
                'amount': [100.0],
                'customer_id': ['1001'],
                'location_id': ['501']
            }),
            npartitions=1
        )
        mock_read_sql.return_value = mock_df

        loader = RedshiftLoader(
            host="test-host",
            database="test-db",
            user="test-user",
            password="test-pass"
        )
        result = loader.load_table("SELECT * FROM table")

        mock_read_sql.assert_called_once()
        assert isinstance(result, dd.DataFrame)

def test_get_loader_bigquery():
    """Test loader factory with BigQuery."""
    with patch('google.auth.default', return_value=(MagicMock(), "test-project")):
        loader = get_loader("bigquery", project_id="test-project")
        assert isinstance(loader, BigQueryLoader)

def test_get_loader_redshift():
    """Test loader factory with Redshift."""
    loader = get_loader(
        "redshift",
        host="test-host",
        database="test-db",
        user="test-user",
        password="test-pass"
    )
    assert isinstance(loader, RedshiftLoader)

def test_get_loader_invalid():
    """Test loader factory with invalid source type."""
    with pytest.raises(ValueError):
        get_loader("invalid_type") 