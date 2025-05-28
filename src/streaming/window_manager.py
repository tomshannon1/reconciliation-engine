"""
Manages time windows for streaming reconciliation.
"""
from datetime import datetime, timedelta
from typing import Tuple, Optional
import time

class WindowingManager:
    """Manages time-based windows for streaming reconciliation."""
    
    def __init__(self, config: dict):
        """
        Initialize the window manager.
        
        Args:
            config: Dictionary containing window configuration
                - type: "tumbling" | "sliding" | "session"
                - size: Window size in seconds or duration string (e.g., "1h")
                - grace: Grace period for late arrivals
        """
        self.window_type = config['type']
        self.window_size = self._parse_duration(config['size'])
        self.grace_period = self._parse_duration(config.get('grace', '10m'))
        
    def _parse_duration(self, duration: str) -> int:
        """Convert duration string to seconds."""
        units = {
            's': 1,
            'm': 60,
            'h': 3600,
            'd': 86400
        }
        
        if isinstance(duration, (int, float)):
            return int(duration)
            
        unit = duration[-1]
        value = int(duration[:-1])
        
        if unit not in units:
            raise ValueError(f"Invalid duration unit: {unit}")
            
        return value * units[unit]
        
    def assign_window(self, timestamp: float) -> Tuple[int, int]:
        """
        Assign a timestamp to a window.
        
        Args:
            timestamp: Unix timestamp
            
        Returns:
            Tuple of (window_start, window_end) timestamps
        """
        if self.window_type == "tumbling":
            window_start = (timestamp // self.window_size) * self.window_size
            return (int(window_start), int(window_start + self.window_size))
        elif self.window_type == "sliding":
            current_time = time.time()
            window_start = current_time - self.window_size
            return (int(window_start), int(current_time))
        else:
            raise ValueError(f"Unsupported window type: {self.window_type}")
            
    def is_window_complete(self, window_end: int) -> bool:
        """
        Check if a window is complete (including grace period).
        
        Args:
            window_end: End timestamp of the window
            
        Returns:
            True if the window is complete and ready for processing
        """
        current_time = time.time()
        return current_time > window_end + self.grace_period
        
    def get_window_id(self, window_start: int, window_end: int) -> str:
        """Generate a unique window identifier."""
        return f"window_{window_start}_{window_end}" 