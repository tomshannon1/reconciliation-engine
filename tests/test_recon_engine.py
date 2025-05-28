import sys
import os

import dask.dataframe as dd
import pandas as pd
from src.recon_engine import ReconciliationEngine

def test_exact_match_with_tolerance():
    # Create internal and external test data
    internal_data = pd.DataFrame({
        "internal_tx_id": ["INT001"],
        "amount": [100.0],
        "date": ["2024-08-01"],
        "customer_id": ["C001"],
        "location_id": ["LOC01"]
    })

    external_data = pd.DataFrame({
        "external_tx_id": ["EXT001"],
        "gross_amount": [105.0],
        "fee": [5.0],
        "net_amount": [100.02],  # Slight mismatch to test amount_tolerance
        "date": ["2024-08-01"],
        "description": ["Payment from C001"],
        "customer_id": ["C001"],
        "location_id": ["LOC01"]
    })

    internal_df = dd.from_pandas(internal_data, npartitions=1)
    external_df = dd.from_pandas(external_data, npartitions=1)

    engine = ReconciliationEngine(
        match_on=["date", "customer_id", "location_id"],  # only exact keys
        internal_key="amount",    # used for filtering, not joining
        external_key="net_amount",
        amount_tolerance=0.05,
        date_tolerance_days=0
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(internal_df, external_df)

    assert matched.compute().shape[0] == 1, "Expected one matched row within tolerance"
    assert unmatched_internal.compute().shape[0] == 0, "Expected zero unmatched internal rows"
    assert unmatched_external.compute().shape[0] == 0, "Expected zero unmatched external rows"

if __name__ == "__main__":
    test_exact_match_with_tolerance()
    print("✅ All tests passed.")
