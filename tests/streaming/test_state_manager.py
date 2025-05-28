"""
Tests for the StateManager class.
"""
import pytest
import json
import shutil
from pathlib import Path
from src.streaming.state_manager import StateManager

@pytest.fixture
def temp_state_dir(tmp_path):
    """Create a temporary directory for state storage."""
    state_dir = tmp_path / "stream_state"
    yield state_dir
    # Cleanup
    if state_dir.exists():
        shutil.rmtree(state_dir)

@pytest.fixture
def state_manager(temp_state_dir):
    """Create a StateManager instance with test configuration."""
    config = {
        'storage_path': str(temp_state_dir),
        'max_windows': 2
    }
    return StateManager(config)

def test_buffer_record(state_manager, sample_records):
    """Test buffering records in memory."""
    window_id = "test_window_1"
    
    # Buffer internal record
    state_manager.buffer_record(window_id, 'internal', sample_records['internal'][0])
    assert len(state_manager.windows[window_id]['internal']) == 1
    
    # Buffer external record
    state_manager.buffer_record(window_id, 'external', sample_records['external'][0])
    assert len(state_manager.windows[window_id]['external']) == 1
    
    # Verify record contents
    assert state_manager.windows[window_id]['internal'][0] == sample_records['internal'][0]
    assert state_manager.windows[window_id]['external'][0] == sample_records['external'][0]

def test_max_windows_persistence(state_manager, sample_records):
    """Test that old windows are persisted when max_windows is reached."""
    # Add records to three windows (max is 2)
    for i in range(3):
        window_id = f"test_window_{i}"
        state_manager.buffer_record(window_id, 'internal', sample_records['internal'][0])
    
    # First window should be persisted to disk
    assert "test_window_0" not in state_manager.windows
    assert len(state_manager.windows) == 2
    
    # Verify persisted window can be retrieved
    records = state_manager.get_window_records("test_window_0")
    assert len(records['internal']) == 1
    assert records['internal'][0] == sample_records['internal'][0]

def test_get_window_records(state_manager, sample_records):
    """Test retrieving window records from memory and disk."""
    window_id = "test_window_1"
    
    # Add records
    state_manager.buffer_record(window_id, 'internal', sample_records['internal'][0])
    state_manager.buffer_record(window_id, 'external', sample_records['external'][0])
    
    # Get from memory
    records = state_manager.get_window_records(window_id)
    assert len(records['internal']) == 1
    assert len(records['external']) == 1
    
    # Force persistence to disk
    state_manager._persist_oldest_window()
    
    # Get from disk
    records = state_manager.get_window_records(window_id)
    assert len(records['internal']) == 1
    assert len(records['external']) == 1
    
    # Test non-existent window
    records = state_manager.get_window_records("nonexistent")
    assert records == {'internal': [], 'external': []}

def test_cleanup_window(state_manager, sample_records):
    """Test window cleanup from both memory and disk."""
    window_id = "test_window_1"
    
    # Add records
    state_manager.buffer_record(window_id, 'internal', sample_records['internal'][0])
    state_manager.buffer_record(window_id, 'external', sample_records['external'][0])
    
    # Force persistence to disk
    state_manager._persist_oldest_window()
    
    # Cleanup window
    state_manager.cleanup_window(window_id)
    
    # Verify cleanup
    assert window_id not in state_manager.windows
    window_path = Path(state_manager.storage_path) / f"{window_id}.json"
    assert not window_path.exists()

def test_persist_oldest_window(state_manager, sample_records):
    """Test persisting the oldest window to disk."""
    # Add records to two windows
    state_manager.buffer_record("window_1", 'internal', sample_records['internal'][0])
    state_manager.buffer_record("window_2", 'internal', sample_records['internal'][1])
    
    # Persist oldest window
    state_manager._persist_oldest_window()
    
    # Verify persistence
    assert "window_1" not in state_manager.windows
    assert "window_2" in state_manager.windows
    
    # Verify file exists
    window_path = Path(state_manager.storage_path) / "window_1.json"
    assert window_path.exists()
    
    # Verify file contents
    with open(window_path, 'r') as f:
        data = json.load(f)
        assert data['internal'][0] == sample_records['internal'][0] 