"""
Common test fixtures for streaming tests.
"""
import pytest
import json
import asyncio
from unittest.mock import MagicMock, AsyncMock
from typing import Dict, Any

@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Sample configuration for streaming tests."""
    return {
        'sources': {
            'internal': {
                'type': 'kafka',
                'connection': {
                    'bootstrap_servers': 'localhost:9092',
                    'security': {
                        'sasl_mechanism': 'PLAIN',
                        'security_protocol': 'SASL_SSL',
                        'username': 'test_user',
                        'password': 'test_pass'
                    }
                },
                'topic': 'internal_transactions',
                'group_id': 'test_internal'
            },
            'external': {
                'type': 'kafka',
                'connection': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'topic': 'external_transactions',
                'group_id': 'test_external'
            }
        },
        'window': {
            'type': 'tumbling',
            'size': '1h',
            'grace': '10m'
        },
        'state': {
            'storage_path': 'tests/data/stream_state',
            'max_windows': 10
        },
        'matching': {
            'match_on': ['date', 'customer_id'],
            'internal_key': 'amount',
            'external_key': 'net_amount',
            'tolerances': {
                'amount': 0.01,
                'days': 1
            }
        }
    }

@pytest.fixture
def sample_records():
    """Sample transaction records for testing."""
    return {
        'internal': [
            {
                'date': '2024-01-01',
                'customer_id': '123',
                'amount': 100.00,
                'transaction_id': 'int_1'
            },
            {
                'date': '2024-01-01',
                'customer_id': '456',
                'amount': 200.00,
                'transaction_id': 'int_2'
            }
        ],
        'external': [
            {
                'date': '2024-01-01',
                'customer_id': '123',
                'net_amount': 100.01,
                'reference': 'ext_1'
            },
            {
                'date': '2024-01-01',
                'customer_id': '789',
                'net_amount': 300.00,
                'reference': 'ext_2'
            }
        ]
    }

@pytest.fixture
def mock_kafka_message():
    """Mock Kafka message for testing."""
    class MockMessage:
        def __init__(self, value, timestamp):
            self.value = value
            self.timestamp = timestamp * 1000  # Kafka uses milliseconds
            
    return MockMessage

@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer for testing."""
    class MockConsumer:
        def __init__(self):
            self.messages = []
            self.started = False
            
        async def start(self):
            self.started = True
            
        async def stop(self):
            self.started = False
            
        async def commit(self):
            pass
            
        def __aiter__(self):
            return self
            
        async def __anext__(self):
            if not self.messages:
                raise StopAsyncIteration
            return self.messages.pop(0)
            
    return MockConsumer 