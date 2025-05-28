"""
Tests for the StreamMetrics class.
"""
import pytest
import time
from src.streaming.metrics import StreamMetrics, WindowMetrics

def test_window_metrics_initialization():
    """Test WindowMetrics initialization."""
    metrics = WindowMetrics()
    
    assert metrics.records_processed == 0
    assert metrics.matches_found == 0
    assert metrics.processing_time == 0.0
    assert isinstance(metrics.start_time, float)

def test_stream_metrics_initialization():
    """Test StreamMetrics initialization."""
    metrics = StreamMetrics()
    
    assert metrics.window_metrics == {}
    assert metrics.consumer_lag == {}
    assert metrics.total_records_processed == 0
    assert metrics.total_matches_found == 0
    assert metrics.avg_processing_time == 0.0

def test_record_processing():
    """Test recording processed records."""
    metrics = StreamMetrics()
    window_id = "test_window"
    
    # Start window tracking
    metrics.start_window(window_id)
    
    # Record some processing
    metrics.record_processed(window_id)
    metrics.record_processed(window_id)
    metrics.record_match(window_id)
    
    # Check window metrics
    assert metrics.window_metrics[window_id].records_processed == 2
    assert metrics.window_metrics[window_id].matches_found == 1
    
    # Check overall metrics
    assert metrics.total_records_processed == 2
    assert metrics.total_matches_found == 1

def test_window_completion():
    """Test window completion metrics."""
    metrics = StreamMetrics()
    window_id = "test_window"
    
    # Start window and add some records
    metrics.start_window(window_id)
    metrics.record_processed(window_id)
    metrics.record_match(window_id)
    
    # Wait a bit to ensure measurable processing time
    time.sleep(0.1)
    
    # End window
    metrics.end_window(window_id)
    
    # Check processing time
    assert metrics.window_metrics[window_id].processing_time > 0
    assert metrics.avg_processing_time > 0

def test_consumer_lag_tracking():
    """Test consumer lag metrics."""
    metrics = StreamMetrics()
    
    # Update lag for different sources and partitions
    metrics.update_lag("internal", 0, 100, 150)  # Lag of 50
    metrics.update_lag("internal", 1, 200, 220)  # Lag of 20
    metrics.update_lag("external", 0, 300, 400)  # Lag of 100
    
    # Check lag metrics
    assert metrics.consumer_lag["internal"][0] == 50
    assert metrics.consumer_lag["internal"][1] == 20
    assert metrics.consumer_lag["external"][0] == 100

def test_window_stats():
    """Test window statistics calculation."""
    metrics = StreamMetrics()
    window_id = "test_window"
    
    # Start window and add records
    metrics.start_window(window_id)
    for _ in range(10):
        metrics.record_processed(window_id)
    for _ in range(6):
        metrics.record_match(window_id)
    
    # Get window stats
    stats = metrics.get_window_stats(window_id)
    
    assert stats['records_processed'] == 10
    assert stats['matches_found'] == 6
    assert stats['match_rate'] == 0.6  # 6/10 = 0.6
    
    # Test non-existent window
    assert metrics.get_window_stats("nonexistent") == {}

def test_overall_stats():
    """Test overall statistics calculation."""
    metrics = StreamMetrics()
    
    # Add data for multiple windows
    for i in range(3):
        window_id = f"window_{i}"
        metrics.start_window(window_id)
        metrics.record_processed(window_id)
        if i % 2 == 0:
            metrics.record_match(window_id)
    
    # Update some lag metrics
    metrics.update_lag("internal", 0, 100, 150)
    
    # Get overall stats
    stats = metrics.get_overall_stats()
    
    assert stats['total_records_processed'] == 3
    assert stats['total_matches_found'] == 2
    assert stats['overall_match_rate'] == 2/3
    assert stats['total_windows_processed'] == 3
    assert stats['consumer_lag']['internal'][0] == 50

def test_multiple_windows():
    """Test handling multiple windows simultaneously."""
    metrics = StreamMetrics()
    
    # Start multiple windows
    windows = ["window_1", "window_2", "window_3"]
    for window_id in windows:
        metrics.start_window(window_id)
        metrics.record_processed(window_id)
        metrics.record_match(window_id)
    
    # End windows in different order
    time.sleep(0.1)
    metrics.end_window("window_2")
    time.sleep(0.1)
    metrics.end_window("window_1")
    time.sleep(0.1)
    metrics.end_window("window_3")
    
    # Check all windows were tracked
    assert len(metrics.window_metrics) == 3
    for window_id in windows:
        assert metrics.window_metrics[window_id].processing_time > 0 