"""
Tests for the StreamProcessor class.
"""
import pytest
import asyncio
import time
from unittest.mock import MagicMock, patch
from src.streaming.stream_processor import StreamProcessor

@pytest.mark.asyncio
async def test_stream_processor_initialization(sample_config):
    """Test StreamProcessor initialization."""
    processor = StreamProcessor(sample_config)
    
    assert processor.window_manager is not None
    assert processor.state_manager is not None
    assert processor.matcher is not None
    assert not processor.running
    assert len(processor.consumers) == 0

@pytest.mark.asyncio
async def test_start_and_stop(sample_config, mock_kafka_consumer):
    """Test starting and stopping the stream processor."""
    processor = StreamProcessor(sample_config)
    
    # Mock Kafka consumer creation
    with patch('src.streaming.stream_processor.AIOKafkaConsumer', mock_kafka_consumer):
        # Start processor
        start_task = asyncio.create_task(processor.start())
        await asyncio.sleep(0.1)  # Give it time to start
        
        assert processor.running
        assert len(processor.consumers) == 2  # internal and external
        
        # Stop processor
        await processor.stop()
        await start_task
        
        assert not processor.running
        for consumer in processor.consumers.values():
            assert not consumer.started

@pytest.mark.asyncio
async def test_process_messages(sample_config, mock_kafka_consumer, mock_kafka_message, sample_records):
    """Test processing Kafka messages."""
    processor = StreamProcessor(sample_config)
    
    # Create mock consumers with test messages
    internal_consumer = mock_kafka_consumer()
    external_consumer = mock_kafka_consumer()
    
    # Add test messages
    timestamp = int(time.time())
    internal_consumer.messages = [
        mock_kafka_message(sample_records['internal'][0], timestamp),
        mock_kafka_message(sample_records['internal'][1], timestamp)
    ]
    external_consumer.messages = [
        mock_kafka_message(sample_records['external'][0], timestamp),
        mock_kafka_message(sample_records['external'][1], timestamp)
    ]
    
    with patch('src.streaming.stream_processor.AIOKafkaConsumer') as mock_consumer:
        # Configure mock consumer factory
        mock_consumer.side_effect = [internal_consumer, external_consumer]
        
        # Start processor
        start_task = asyncio.create_task(processor.start())
        await asyncio.sleep(0.1)  # Give it time to process messages
        
        # Stop processor
        await processor.stop()
        await start_task
        
        # Verify messages were processed
        window_start, window_end = processor.window_manager.assign_window(timestamp)
        window_id = processor.window_manager.get_window_id(window_start, window_end)
        
        records = processor.state_manager.get_window_records(window_id)
        assert len(records['internal']) == 2
        assert len(records['external']) == 2

@pytest.mark.asyncio
async def test_window_reconciliation(sample_config, mock_kafka_consumer, mock_kafka_message, sample_records):
    """Test reconciliation of complete windows."""
    processor = StreamProcessor(sample_config)
    
    # Create mock consumer with messages from the past
    consumer = mock_kafka_consumer()
    old_timestamp = int(time.time()) - 7200  # 2 hours ago
    consumer.messages = [
        mock_kafka_message(sample_records['internal'][0], old_timestamp),
        mock_kafka_message(sample_records['external'][0], old_timestamp)
    ]
    
    with patch('src.streaming.stream_processor.AIOKafkaConsumer', return_value=consumer):
        # Start processor
        start_task = asyncio.create_task(processor.start())
        await asyncio.sleep(0.1)  # Give it time to process
        
        # Stop processor
        await processor.stop()
        await start_task
        
        # Verify window was reconciled (cleaned up)
        window_start, window_end = processor.window_manager.assign_window(old_timestamp)
        window_id = processor.window_manager.get_window_id(window_start, window_end)
        
        records = processor.state_manager.get_window_records(window_id)
        assert records == {'internal': [], 'external': []}  # Window should be cleaned up

@pytest.mark.asyncio
async def test_error_handling(sample_config, mock_kafka_consumer):
    """Test error handling in stream processing."""
    processor = StreamProcessor(sample_config)
    
    # Create mock consumer that raises an exception
    consumer = mock_kafka_consumer()
    consumer.start = AsyncMock(side_effect=Exception("Kafka connection error"))
    
    with patch('src.streaming.stream_processor.AIOKafkaConsumer', return_value=consumer):
        with pytest.raises(Exception, match="Kafka connection error"):
            await processor.start()
        
        assert not processor.running 