import pytest
import os
import logging
import yaml
from main import load_csv, load_config
from botocore.exceptions import NoCredentialsError
from google.auth.exceptions import DefaultCredentialsError

def test_load_csv_local_csv(sample_csv_data):
    """Test loading a local CSV file."""
    df = load_csv(sample_csv_data)
    result = df.compute()
    assert len(result) == 2
    assert list(result.columns) == ['date', 'customer_id', 'location_id', 'amount']
    assert result['amount'].iloc[0] == 100.50

def test_load_csv_local_psv(sample_psv_data):
    """Test loading a local PSV file."""
    df = load_csv(sample_psv_data)
    result = df.compute()
    assert len(result) == 2
    assert list(result.columns) == ['date', 'customer_id', 'location_id', 'amount']
    assert result['amount'].iloc[0] == 100.50

def test_load_csv_file_not_found():
    """Test handling of non-existent file."""
    with pytest.raises(FileNotFoundError):
        load_csv("nonexistent_file.csv")

def test_load_csv_s3(mock_s3_file):
    """Test loading a file from S3."""
    df = load_csv(mock_s3_file)
    result = df.compute()
    assert len(result) == 1
    assert 'date' in result.columns
    assert 'amount' in result.columns

def test_load_csv_s3_no_credentials(monkeypatch):
    """Test handling of missing S3 credentials."""
    def mock_s3fs_init(*args, **kwargs):
        raise NoCredentialsError()
    
    monkeypatch.setattr("s3fs.S3FileSystem.__init__", mock_s3fs_init)
    
    with pytest.raises(NoCredentialsError):
        load_csv("s3://bucket/file.csv")

def test_load_config_valid(sample_config_file):
    """Test loading a valid config file."""
    config = load_config(sample_config_file)
    assert config["matching"]["match_on"] == ["date", "customer_id", "location_id"]
    assert config["tolerances"]["amount"] == 0.01
    assert config["tolerances"]["days"] == 1
    assert config["output"]["path"] == "test_output"

def test_load_config_file_not_found():
    """Test handling of non-existent config file."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent_config.yaml")

def test_load_config_invalid_yaml(tmp_path):
    """Test handling of invalid YAML file."""
    invalid_yaml = tmp_path / "invalid.yaml"
    invalid_yaml.write_text("invalid: yaml: content: [")
    
    with pytest.raises(yaml.YAMLError):
        load_config(str(invalid_yaml))

def test_load_config_missing_required_fields(tmp_path):
    """Test handling of config file with missing required fields."""
    incomplete_config = tmp_path / "incomplete.yaml"
    incomplete_config.write_text("""
    matching:
      internal_key: amount
    """)
    
    with pytest.raises(KeyError):
        config = load_config(str(incomplete_config))
        # Access required fields to trigger validation
        _ = config["matching"]["match_on"]

def test_logging_configuration(caplog):
    """Test that logging is properly configured."""
    with caplog.at_level(logging.INFO):
        try:
            load_csv("nonexistent_file.csv")
        except FileNotFoundError:
            pass
    
    # Check that we logged the attempt
    assert any("Attempting to read file" in record.message for record in caplog.records)
    # Check that we logged the error
    assert any("Failed to load file" in record.message for record in caplog.records)

def test_load_csv_gcs(mock_gcs_file):
    """Test loading a file from GCS."""
    df = load_csv(mock_gcs_file)
    result = df.compute()
    assert len(result) == 1
    assert 'date' in result.columns
    assert 'amount' in result.columns

def test_load_csv_gcs_no_credentials(monkeypatch):
    """Test handling of missing GCS credentials."""
    def mock_gcsfs_init(*args, **kwargs):
        raise DefaultCredentialsError("Could not automatically determine credentials")
    
    monkeypatch.setattr("gcsfs.GCSFileSystem.__init__", mock_gcsfs_init)
    
    with pytest.raises(DefaultCredentialsError):
        load_csv("gs://bucket/file.csv")

def test_load_csv_gcs_file_not_found(monkeypatch):
    """Test handling of non-existent GCS file."""
    class MockGCSFileSystem:
        def exists(self, path):
            return False
    
    monkeypatch.setattr("gcsfs.GCSFileSystem", MockGCSFileSystem)
    
    with pytest.raises(FileNotFoundError):
        load_csv("gs://bucket/nonexistent.csv") 