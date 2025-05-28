"""
Manages state for windowed streaming reconciliation.
"""
from typing import Dict, List, Any
import json
import os
from pathlib import Path

class StateManager:
    """Manages state for windowed streaming data."""
    
    def __init__(self, config: dict):
        """
        Initialize the state manager.
        
        Args:
            config: Dictionary containing state configuration
                - storage_path: Path to store window state
                - max_windows: Maximum number of windows to keep in memory
        """
        self.storage_path = Path(config.get('storage_path', 'data/stream_state'))
        self.max_windows = config.get('max_windows', 100)
        self.windows: Dict[str, Dict[str, List[dict]]] = {}
        
        # Create storage directory if it doesn't exist
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
    def buffer_record(self, window_id: str, source: str, record: dict):
        """
        Buffer a record for a specific window.
        
        Args:
            window_id: Unique window identifier
            source: Source identifier (e.g., 'internal' or 'external')
            record: Record data to buffer
        """
        if window_id not in self.windows:
            self.windows[window_id] = {'internal': [], 'external': []}
            
        self.windows[window_id][source].append(record)
        
        # If we have too many windows, persist oldest to disk
        if len(self.windows) > self.max_windows:
            self._persist_oldest_window()
            
    def get_window_records(self, window_id: str) -> Dict[str, List[dict]]:
        """
        Get all records for a specific window.
        
        Args:
            window_id: Window identifier
            
        Returns:
            Dictionary containing internal and external records
        """
        # Try memory first
        if window_id in self.windows:
            return self.windows[window_id]
            
        # Try disk
        window_path = self.storage_path / f"{window_id}.json"
        if window_path.exists():
            with open(window_path, 'r') as f:
                return json.load(f)
                
        return {'internal': [], 'external': []}
        
    def cleanup_window(self, window_id: str):
        """
        Remove window data from memory and disk.
        
        Args:
            window_id: Window identifier to cleanup
        """
        # Remove from memory
        if window_id in self.windows:
            del self.windows[window_id]
            
        # Remove from disk
        window_path = self.storage_path / f"{window_id}.json"
        if window_path.exists():
            window_path.unlink()
            
    def _persist_oldest_window(self):
        """Persist the oldest window to disk and remove from memory."""
        if not self.windows:
            return
            
        # Find oldest window
        oldest_id = min(self.windows.keys())
        window_data = self.windows[oldest_id]
        
        # Write to disk
        window_path = self.storage_path / f"{oldest_id}.json"
        with open(window_path, 'w') as f:
            json.dump(window_data, f)
            
        # Remove from memory
        del self.windows[oldest_id] 