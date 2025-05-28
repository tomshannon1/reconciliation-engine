"""
Metrics tracking for streaming reconciliation.
"""
from typing import Dict, Any
import time
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class WindowMetrics:
    """Metrics for a single window."""
    records_processed: int = 0
    matches_found: int = 0
    processing_time: float = 0.0
    start_time: float = field(default_factory=time.time)

class StreamMetrics:
    """Tracks metrics for streaming reconciliation."""
    
    def __init__(self):
        """Initialize metrics tracking."""
        self.window_metrics: Dict[str, WindowMetrics] = {}
        self.consumer_lag: Dict[str, Dict[int, int]] = defaultdict(dict)
        self.total_records_processed = 0
        self.total_matches_found = 0
        self.avg_processing_time = 0.0
        
    def start_window(self, window_id: str):
        """Start tracking metrics for a window."""
        self.window_metrics[window_id] = WindowMetrics()
        
    def record_processed(self, window_id: str):
        """Record a processed message."""
        if window_id in self.window_metrics:
            self.window_metrics[window_id].records_processed += 1
        self.total_records_processed += 1
        
    def record_match(self, window_id: str):
        """Record a matched transaction."""
        if window_id in self.window_metrics:
            self.window_metrics[window_id].matches_found += 1
        self.total_matches_found += 1
        
    def end_window(self, window_id: str):
        """End tracking metrics for a window."""
        if window_id in self.window_metrics:
            metrics = self.window_metrics[window_id]
            metrics.processing_time = time.time() - metrics.start_time
            
            # Update average processing time
            total_windows = len(self.window_metrics)
            self.avg_processing_time = (
                (self.avg_processing_time * (total_windows - 1) + metrics.processing_time)
                / total_windows
            )
            
    def update_lag(self, source: str, partition: int, current_offset: int, end_offset: int):
        """Update consumer lag metrics."""
        self.consumer_lag[source][partition] = end_offset - current_offset
        
    def get_window_stats(self, window_id: str) -> Dict[str, Any]:
        """Get statistics for a specific window."""
        if window_id not in self.window_metrics:
            return {}
            
        metrics = self.window_metrics[window_id]
        return {
            'records_processed': metrics.records_processed,
            'matches_found': metrics.matches_found,
            'processing_time': metrics.processing_time,
            'match_rate': (
                metrics.matches_found / metrics.records_processed
                if metrics.records_processed > 0 else 0
            )
        }
        
    def get_overall_stats(self) -> Dict[str, Any]:
        """Get overall streaming statistics."""
        return {
            'total_records_processed': self.total_records_processed,
            'total_matches_found': self.total_matches_found,
            'avg_processing_time': self.avg_processing_time,
            'total_windows_processed': len(self.window_metrics),
            'overall_match_rate': (
                self.total_matches_found / self.total_records_processed
                if self.total_records_processed > 0 else 0
            ),
            'consumer_lag': dict(self.consumer_lag)
        } 