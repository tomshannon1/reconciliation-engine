"""
Tests for the WindowManager class.
"""
import pytest
import time
from src.streaming.window_manager import WindowingManager

def test_parse_duration():
    """Test duration string parsing."""
    config = {'type': 'tumbling', 'size': '1h', 'grace': '10m'}
    manager = WindowingManager(config)
    
    assert manager.window_size == 3600  # 1h in seconds
    assert manager.grace_period == 600  # 10m in seconds
    
    # Test invalid duration
    with pytest.raises(ValueError):
        WindowingManager({'type': 'tumbling', 'size': '1x'})

def test_tumbling_window_assignment():
    """Test tumbling window assignment."""
    config = {'type': 'tumbling', 'size': '1h'}
    manager = WindowingManager(config)
    
    # Test exact hour boundary
    timestamp = 1704067200  # 2024-01-01 00:00:00
    start, end = manager.assign_window(timestamp)
    assert start == 1704067200  # 2024-01-01 00:00:00
    assert end == 1704070800   # 2024-01-01 01:00:00
    
    # Test mid-hour timestamp
    timestamp = 1704069000  # 2024-01-01 00:30:00
    start, end = manager.assign_window(timestamp)
    assert start == 1704067200  # 2024-01-01 00:00:00
    assert end == 1704070800   # 2024-01-01 01:00:00

def test_sliding_window_assignment():
    """Test sliding window assignment."""
    config = {'type': 'sliding', 'size': '1h'}
    manager = WindowingManager(config)
    
    current_time = time.time()
    start, end = manager.assign_window(current_time)
    
    assert end == int(current_time)
    assert start == int(current_time - 3600)

def test_window_completion():
    """Test window completion check."""
    config = {'type': 'tumbling', 'size': '1h', 'grace': '10m'}
    manager = WindowingManager(config)
    
    current_time = time.time()
    
    # Window + grace period not yet passed
    assert not manager.is_window_complete(current_time)
    
    # Window + grace period passed
    assert manager.is_window_complete(current_time - 4200)  # 70 minutes ago

def test_window_id_generation():
    """Test window ID generation."""
    config = {'type': 'tumbling', 'size': '1h'}
    manager = WindowingManager(config)
    
    window_id = manager.get_window_id(1704067200, 1704070800)
    assert window_id == "window_1704067200_1704070800"

def test_invalid_window_type():
    """Test invalid window type handling."""
    config = {'type': 'invalid', 'size': '1h'}
    manager = WindowingManager(config)
    
    with pytest.raises(ValueError, match="Unsupported window type: invalid"):
        manager.assign_window(time.time()) 