import sys
import os

import dask.dataframe as dd
from src.recon_engine import ReconciliationEngine

def test_exact_match_with_tolerance(sample_internal_data, sample_external_data):
    """Test that transactions match when within the specified tolerance."""
    engine = ReconciliationEngine(
        match_on=["date", "customer_id", "location_id"],  # only exact keys
        internal_key="amount",    # used for filtering, not joining
        external_key="net_amount",
        amount_tolerance=0.05,
        date_tolerance_days=0
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(
        sample_internal_data, 
        sample_external_data
    )

    assert matched.compute().shape[0] == 1, "Expected one matched row within tolerance"
    assert unmatched_internal.compute().shape[0] == 0, "Expected zero unmatched internal rows"
    assert unmatched_external.compute().shape[0] == 0, "Expected zero unmatched external rows"

if __name__ == "__main__":
    test_exact_match_with_tolerance()
    print("✅ All tests passed.")
