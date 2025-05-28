"""
Pytest configuration and fixtures.
"""
import pytest
import pandas as pd
import dask.dataframe as dd
import yaml
import tempfile
import os
from datetime import datetime, timedelta
from google.auth.exceptions import DefaultCredentialsError

@pytest.fixture
def sample_internal_data():
    return dd.from_dict({
        "internal_tx_id": ["INT001"],
        "amount": [100.0],
        "date": ["2024-08-01"],
        "customer_id": ["C001"],
        "location_id": ["LOC01"]
    }, npartitions=1)

@pytest.fixture
def sample_external_data():
    return dd.from_dict({
        "external_tx_id": ["EXT001"],
        "gross_amount": [105.0],
        "fee": [5.0],
        "net_amount": [100.02],
        "date": ["2024-08-01"],
        "description": ["Payment from C001"],
        "customer_id": ["C001"],
        "location_id": ["LOC01"]
    }, npartitions=1)

@pytest.fixture
def sample_csv_data():
    """Create a temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("date,customer_id,location_id,amount\n")
        f.write("2024-03-21,1001,501,100.50\n")
        f.write("2024-03-22,1002,502,200.75\n")
    yield f.name
    os.unlink(f.name)

@pytest.fixture
def sample_psv_data():
    """Create a temporary PSV file with sample data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.psv', delete=False) as f:
        f.write("date|customer_id|location_id|amount\n")
        f.write("2024-03-21|1001|501|100.50\n")
        f.write("2024-03-22|1002|502|200.75\n")
    yield f.name
    os.unlink(f.name)

@pytest.fixture
def sample_config_file():
    """Create a temporary config file."""
    config_data = {
        "matching": {
            "match_on": ["date", "customer_id", "location_id"],
            "internal_key": "amount",
            "external_key": "net_amount"
        },
        "tolerances": {
            "amount": 0.01,
            "days": 1
        },
        "output": {
            "path": "test_output",
            "reconciled_file": "matched.csv",
            "unmatched_internal_file": "unmatched_internal.csv",
            "unmatched_external_file": "unmatched_external.csv"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
    yield f.name
    os.unlink(f.name)

@pytest.fixture
def mock_s3_file(monkeypatch):
    """Mock S3 file path and filesystem."""
    class MockS3FileSystem:
        def exists(self, path):
            return True
    
    def mock_read_csv(*args, **kwargs):
        data = {
            'date': [datetime.now()],
            'amount': [100.0],
            'customer_id': [1001],
            'location_id': [501]
        }
        return dd.from_pandas(pd.DataFrame(data), npartitions=1)
    
    monkeypatch.setattr("s3fs.S3FileSystem", MockS3FileSystem)
    monkeypatch.setattr("dask.dataframe.read_csv", mock_read_csv)
    
    return "s3://test-bucket/data.csv"

@pytest.fixture
def mock_gcs_file(monkeypatch):
    """Mock GCS file path and filesystem."""
    class MockGCSFileSystem:
        def exists(self, path):
            return True
    
    def mock_read_csv(*args, **kwargs):
        data = {
            'date': [datetime.now()],
            'amount': [100.0],
            'customer_id': [1001],
            'location_id': [501]
        }
        return dd.from_pandas(pd.DataFrame(data), npartitions=1)
    
    monkeypatch.setattr("gcsfs.GCSFileSystem", MockGCSFileSystem)
    monkeypatch.setattr("dask.dataframe.read_csv", mock_read_csv)
    
    return "gs://test-bucket/data.csv" 