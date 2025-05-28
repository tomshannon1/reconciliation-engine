"""
End-to-end tests for the reconciliation engine.
"""
import os
import tempfile
import shutil
from scripts.generate_test_data import generate_test_data
from src.recon_engine import ReconciliationEngine
from src.config_validator import ConfigValidator
from src.source_loader import SourceLoader
import pandas as pd
import pytest

@pytest.fixture
def test_data_dir():
    """Create a temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def test_config(test_data_dir):
    """Create a test configuration."""
    config = {
        "sources": {
            "internal": {
                "type": "file",
                "path": f"{test_data_dir}/internal_transactions.csv"
            },
            "external": {
                "type": "file",
                "path": f"{test_data_dir}/external_transactions.csv"
            }
        },
        "matching": {
            "match_on": ["date", "customer_id", "location_id"],
            "internal_key": "amount",
            "external_key": "net_amount"
        },
        "tolerances": {
            "amount": 0.05,  # Match our data generator's variance
            "days": 1        # Match our data generator's variance
        },
        "output": {
            "path": test_data_dir,
            "reconciled_file": "matched.csv",
            "unmatched_internal_file": "unmatched_internal.csv",
            "unmatched_external_file": "unmatched_external.csv"
        }
    }
    return config

def test_reconciliation_matches_generated_data(test_data_dir, test_config):
    """
    Test that the reconciliation engine correctly identifies the expected
    number of matches from our generated test data.
    """
    # Generate test data with known match rate
    num_records = 1000  # Smaller dataset for faster tests
    match_rate = 0.85
    amount_variance_rate = 0.10
    date_variance_rate = 0.05
    id_variance_rate = 0.05
    
    generate_test_data(
        num_records=num_records,
        match_rate=match_rate,
        amount_variance_rate=amount_variance_rate,
        date_variance_rate=date_variance_rate,
        id_variance_rate=id_variance_rate,
        output_dir=test_data_dir
    )
    
    # Calculate expected matches
    expected_matching = int(num_records * match_rate)
    expected_matching_with_variances = expected_matching * (
        1 -  # Start with all matching records
        (amount_variance_rate * 0.5) -  # Assume half of amount variances exceed tolerance
        (date_variance_rate * 0.5) -    # Assume half of date variances exceed tolerance
        id_variance_rate                # All ID variances should fail to match
    )
    
    # Load data sources
    internal_df = SourceLoader.load_source(test_config["sources"]["internal"])
    external_df = SourceLoader.load_source(test_config["sources"]["external"])
    
    # Run reconciliation
    engine = ReconciliationEngine(
        match_on=test_config["matching"]["match_on"],
        internal_key=test_config["matching"]["internal_key"],
        external_key=test_config["matching"]["external_key"],
        amount_tolerance=test_config["tolerances"]["amount"],
        date_tolerance_days=test_config["tolerances"]["days"]
    )
    
    matched, unmatched_internal, unmatched_external = engine.reconcile(internal_df, external_df)
    
    # Get actual match count
    actual_matches = len(matched.compute())
    
    # Verify results
    tolerance = int(expected_matching * 0.05)  # Allow 5% tolerance in our expectation
    assert abs(actual_matches - expected_matching_with_variances) <= tolerance, (
        f"Expected approximately {expected_matching_with_variances} matches "
        f"(±{tolerance}), but got {actual_matches}"
    )
    
    # Verify unmatched counts
    total_records = len(internal_df.compute())
    assert len(unmatched_internal.compute()) + actual_matches == total_records, (
        "Sum of matched and unmatched internal records should equal total records"
    )
    
    # Save results for inspection
    output_path = test_config["output"]["path"]
    matched.compute().to_csv(f"{output_path}/matched.csv", index=False)
    unmatched_internal.compute().to_csv(f"{output_path}/unmatched_internal.csv", index=False)
    unmatched_external.compute().to_csv(f"{output_path}/unmatched_external.csv", index=False)
    
    # Log detailed results for debugging
    print(f"\nTest Results:")
    print(f"Total records: {num_records}")
    print(f"Expected matching records: {expected_matching}")
    print(f"Expected matches after variances: {expected_matching_with_variances}")
    print(f"Actual matches: {actual_matches}")
    print(f"Unmatched internal: {len(unmatched_internal.compute())}")
    print(f"Unmatched external: {len(unmatched_external.compute())}") 