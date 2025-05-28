"""
Pytest configuration and fixtures.
"""
import pytest
import dask.dataframe as dd

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