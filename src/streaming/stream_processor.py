"""
Handles Kafka stream processing for reconciliation.
"""
from typing import Dict, Any, Optional
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from .window_manager import WindowingManager
from .state_manager import StateManager
from ..matcher import TransactionMatcher

class StreamProcessor:
    """Processes Kafka streams for reconciliation."""
    
    def __init__(self, config: dict):
        """
        Initialize the stream processor.
        
        Args:
            config: Dictionary containing stream configuration
                - sources: Kafka source configurations
                - window: Window configuration
                - state: State management configuration
                - matching: Matching configuration
        """
        self.config = config
        self.window_manager = WindowingManager(config['window'])
        self.state_manager = StateManager(config['state'])
        self.matcher = TransactionMatcher(config['matching'])
        self.consumers: Dict[str, AIOKafkaConsumer] = {}
        self.running = False
        
    async def start(self):
        """Start processing streams."""
        # Initialize Kafka consumers
        for source, cfg in self.config['sources'].items():
            if cfg['type'] != 'kafka':
                continue
                
            consumer = AIOKafkaConsumer(
                cfg['topic'],
                bootstrap_servers=cfg['connection']['bootstrap_servers'],
                group_id=cfg['group_id'],
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            # Add SASL config if present
            if 'security' in cfg['connection']:
                security = cfg['connection']['security']
                consumer.config.update({
                    'sasl_mechanism': security.get('sasl_mechanism'),
                    'security_protocol': security.get('security_protocol'),
                    'sasl_plain_username': security.get('username'),
                    'sasl_plain_password': security.get('password')
                })
                
            await consumer.start()
            self.consumers[source] = consumer
            
        self.running = True
        await self._process_streams()
        
    async def stop(self):
        """Stop processing streams."""
        self.running = False
        for consumer in self.consumers.values():
            await consumer.stop()
            
    async def _process_streams(self):
        """Main stream processing loop."""
        try:
            while self.running:
                # Process each source
                for source, consumer in self.consumers.items():
                    async for msg in consumer:
                        # Assign message to window
                        window_start, window_end = self.window_manager.assign_window(
                            msg.timestamp / 1000  # Kafka timestamps are in milliseconds
                        )
                        window_id = self.window_manager.get_window_id(window_start, window_end)
                        
                        # Buffer the record
                        self.state_manager.buffer_record(
                            window_id,
                            source,
                            msg.value
                        )
                        
                        # Check if window is complete
                        if self.window_manager.is_window_complete(window_end):
                            await self._reconcile_window(window_id)
                            
                        # Commit offset
                        await consumer.commit()
                        
                # Small delay to prevent tight loop
                await asyncio.sleep(0.1)
                
        except Exception as e:
            print(f"Error processing streams: {e}")
            raise
            
    async def _reconcile_window(self, window_id: str):
        """
        Reconcile records in a complete window.
        
        Args:
            window_id: Window identifier to reconcile
        """
        # Get window records
        records = self.state_manager.get_window_records(window_id)
        
        # Perform reconciliation
        results = self.matcher.match_transactions(
            records['internal'],
            records['external']
        )
        
        # TODO: Output results (implement in output manager)
        print(f"Window {window_id} reconciliation results:", results)
        
        # Cleanup window data
        self.state_manager.cleanup_window(window_id) 