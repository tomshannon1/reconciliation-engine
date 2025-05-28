import sys
import os

import dask.dataframe as dd
from src.recon_engine import ReconciliationEngine
import pytest
import pandas as pd
from datetime import datetime, timedelta

@pytest.fixture
def sample_internal_data():
    now = datetime.now()
    data = {
        'date': [now, now + timedelta(days=1)],
        'customer_id': ['1001', '1002'],
        'location_id': ['501', '502'],
        'amount': [100.00, 200.00]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    return dd.from_pandas(df, npartitions=1)

@pytest.fixture
def sample_external_data():
    now = datetime.now()
    data = {
        'date': [now, now + timedelta(days=1)],
        'customer_id': ['1001', '1003'],
        'location_id': ['501', '503'],
        'net_amount': [100.02, 300.00]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    return dd.from_pandas(df, npartitions=1)

def test_exact_match_with_tolerance(sample_internal_data, sample_external_data):
    """Test that transactions match when within the specified tolerance."""
    engine = ReconciliationEngine(
        match_on=["customer_id", "location_id"],  # Remove date from matching to ensure consistent test
        internal_key="amount",
        external_key="net_amount",
        amount_tolerance=0.05,
        date_tolerance_days=1
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(
        sample_internal_data, 
        sample_external_data
    )

    matched_df = matched.compute()
    assert matched_df.shape[0] == 1, "Expected one matched row within tolerance"
    assert abs(matched_df['amount'].iloc[0] - matched_df['net_amount'].iloc[0]) <= 0.05, "Match should be within tolerance"

def test_date_tolerance(sample_internal_data, sample_external_data):
    """Test matching with date tolerance."""
    # Modify the external date to be one day off
    external_data = sample_external_data.compute()
    external_data['date'] = external_data['date'].apply(lambda x: x + timedelta(days=1))
    external_data = dd.from_pandas(external_data, npartitions=1)

    engine = ReconciliationEngine(
        match_on=["customer_id", "location_id"],
        internal_key="amount",
        external_key="net_amount",
        amount_tolerance=0.05,
        date_tolerance_days=2
    )

    matched, _, _ = engine.reconcile(sample_internal_data, external_data)
    assert matched.compute().shape[0] == 1, "Expected one match with date tolerance"

def test_no_matches():
    """Test case where no matches are found."""
    internal_data = pd.DataFrame({
        'date': [datetime.now()],
        'customer_id': ['1001'],
        'location_id': ['501'],
        'amount': [100.00]
    })
    external_data = pd.DataFrame({
        'date': [datetime.now()],
        'customer_id': ['1002'],  # Different customer
        'location_id': ['502'],   # Different location
        'net_amount': [100.00]
    })

    engine = ReconciliationEngine(
        match_on=["customer_id", "location_id"],
        internal_key="amount",
        external_key="net_amount"
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(
        dd.from_pandas(internal_data, npartitions=1),
        dd.from_pandas(external_data, npartitions=1)
    )

    assert matched.compute().shape[0] == 0, "Expected no matches"
    assert unmatched_internal.compute().shape[0] == 1, "Expected one unmatched internal"
    assert unmatched_external.compute().shape[0] == 1, "Expected one unmatched external"

def test_amount_tolerance_boundary():
    """Test matching at the exact boundary of amount tolerance."""
    now = datetime.now()
    internal_data = pd.DataFrame({
        'date': [now],
        'customer_id': ['1001'],
        'location_id': ['501'],
        'amount': [100.00]
    })
    internal_data['date'] = pd.to_datetime(internal_data['date'])
    
    external_data = pd.DataFrame({
        'date': [now],
        'customer_id': ['1001'],
        'location_id': ['501'],
        'net_amount': [100.10]  # Difference of 0.10
    })
    external_data['date'] = pd.to_datetime(external_data['date'])

    # Test with tolerance exactly 0.10
    engine = ReconciliationEngine(
        match_on=["customer_id", "location_id"],  # Remove date from matching to ensure consistent test
        internal_key="amount",
        external_key="net_amount",
        amount_tolerance=0.10,
        date_tolerance_days=1
    )

    matched, _, _ = engine.reconcile(
        dd.from_pandas(internal_data, npartitions=1),
        dd.from_pandas(external_data, npartitions=1)
    )

    matched_df = matched.compute()
    assert matched_df.shape[0] == 1, "Expected match at tolerance boundary"
    assert abs(matched_df['amount'].iloc[0] - matched_df['net_amount'].iloc[0]) <= 0.10, "Match should be at tolerance boundary"

    # Test with tolerance 0.09 (should not match)
    engine = ReconciliationEngine(
        match_on=["customer_id", "location_id"],
        internal_key="amount",
        external_key="net_amount",
        amount_tolerance=0.09
    )

    matched, _, _ = engine.reconcile(
        dd.from_pandas(internal_data, npartitions=1),
        dd.from_pandas(external_data, npartitions=1)
    )

    assert matched.compute().shape[0] == 0, "Expected no match just outside tolerance"

if __name__ == "__main__":
    pytest.main([__file__])
