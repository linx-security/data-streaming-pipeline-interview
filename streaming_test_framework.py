"""
Streaming Test Framework - Supporting Infrastructure
DO NOT MODIFY THIS FILE

This module contains all the supporting classes and test infrastructure
for the stream processing coding test.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

from pydantic import BaseModel, validator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# =============================================================================
# MODELS
# =============================================================================


class DataSource(Enum):
    USER_EVENTS = "user_events"
    TRANSACTION_DATA = "transaction_data"
    SYSTEM_METRICS = "system_metrics"
    SECURITY_LOGS = "security_logs"


class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class StreamData:
    """Raw data from streaming sources"""

    source: DataSource
    timestamp: float
    priority: Priority
    data: Dict[str, Any]
    sequence_id: int


class ProcessedRecord(BaseModel):
    """Validated and transformed data"""

    source: str
    timestamp: float
    priority: int
    processed_at: float
    transformed_data: Dict[str, Any]
    processing_time_ms: float

    @validator("priority")
    def validate_priority(cls, v):
        if not 1 <= v <= 4:
            raise ValueError("Priority must be between 1 and 4")
        return v


class PipelineMetrics(BaseModel):
    """Real-time pipeline processing metrics"""

    total_processed: int
    processing_rate_per_second: float
    avg_processing_time_ms: float
    error_rate: float
    backlog_size: int
    rate_limit_hits: int
    by_source: Dict[str, Dict[str, Any]]
    by_priority: Dict[str, int]


class RateLimitError(Exception):
    """Raised when rate limit is exceeded"""

    pass


class ProcessingError(Exception):
    """Raised when data processing fails"""

    pass


# =============================================================================
# SIMULATED DOWNSTREAM API
# =============================================================================


class DownstreamAPI:
    """Simulated downstream API with rate limiting and failures"""

    def __init__(self, max_requests_per_second: int = 10, failure_rate: float = 0.1):
        self.max_requests_per_second = max_requests_per_second
        self.failure_rate = failure_rate
        self.request_timestamps = []
        self.total_requests = 0

    async def send_data(self, record: ProcessedRecord) -> bool:
        """
        Send processed data to downstream API.

        Returns:
            bool: True if successful, raises exception if failed

        Raises:
            RateLimitError: If rate limit exceeded
            ProcessingError: If API call fails
        """
        self.total_requests += 1
        current_time = time.time()

        # Remove old timestamps (older than 1 second)
        self.request_timestamps = [ts for ts in self.request_timestamps if current_time - ts < 1.0]

        # Check rate limit
        if len(self.request_timestamps) >= self.max_requests_per_second:
            raise RateLimitError("Rate limit exceeded")

        # Simulate network delay
        await asyncio.sleep(random.uniform(0.01, 0.05))

        # Simulate random failures
        if random.random() < self.failure_rate:
            raise ProcessingError("Downstream API failure")

        self.request_timestamps.append(current_time)
        return True


# =============================================================================
# DATA STREAM SIMULATOR
# =============================================================================


class DataStreamSimulator:
    """Simulates real-time data streams from multiple sources"""

    def __init__(self, sources: List[DataSource], rate_per_source: int = 5):
        self.sources = sources
        self.rate_per_source = rate_per_source
        self.sequence_counters = {source: 0 for source in sources}
        self.is_running = False

    async def generate_stream(self) -> AsyncGenerator[StreamData, None]:
        """Generate continuous stream of data from all sources"""
        self.is_running = True

        while self.is_running:
            # Generate data from each source
            for source in self.sources:
                if random.random() < 0.8:  # 80% chance of data from each source
                    self.sequence_counters[source] += 1

                    yield StreamData(
                        source=source,
                        timestamp=time.time(),
                        priority=random.choice(list(Priority)),
                        data=self._generate_sample_data(source),
                        sequence_id=self.sequence_counters[source],
                    )

            # Control the rate
            await asyncio.sleep(1.0 / (self.rate_per_source * len(self.sources)))

    def stop(self):
        """Stop the data stream"""
        self.is_running = False

    def _generate_sample_data(self, source: DataSource) -> Dict[str, Any]:
        """Generate realistic sample data for each source type"""
        if source == DataSource.USER_EVENTS:
            return {
                "user_id": f"user_{random.randint(1, 1000)}",
                "event_type": random.choice(["login", "logout", "purchase", "view"]),
                "value": random.randint(1, 500),
            }
        elif source == DataSource.TRANSACTION_DATA:
            return {
                "transaction_id": f"txn_{random.randint(10000, 99999)}",
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "currency": random.choice(["USD", "EUR", "GBP"]),
            }
        elif source == DataSource.SYSTEM_METRICS:
            return {
                "cpu_usage": round(random.uniform(0.0, 100.0), 1),
                "memory_usage": round(random.uniform(0.0, 100.0), 1),
                "disk_io": random.randint(0, 1000),
            }
        else:  # SECURITY_LOGS
            return {
                "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                "threat_level": random.choice(["low", "medium", "high"]),
                "blocked": random.choice([True, False]),
            }


# =============================================================================
# TEST RUNNER
# =============================================================================


async def run_stream_test(processor_class):
    """Test the stream processor implementation"""
    print("🚀 Testing Stream Processing Pipeline...")

    # Setup
    downstream_api = DownstreamAPI(max_requests_per_second=15, failure_rate=0.15)
    processor = processor_class(downstream_api, max_concurrent_streams=4)

    # Create data streams
    sources = [DataSource.USER_EVENTS, DataSource.TRANSACTION_DATA, DataSource.SYSTEM_METRICS]
    simulator = DataStreamSimulator(sources, rate_per_source=3)

    try:
        # Start processing
        print("📊 Starting stream processing for 10 seconds...")

        # Run processing for limited time
        stream_task = asyncio.create_task(processor.start_processing(simulator.generate_stream()))

        # Let it run for 10 seconds
        await asyncio.sleep(10)

        # Stop and get final metrics
        simulator.stop()
        await processor.stop_processing()

        # Cancel the stream task
        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass

        # Get final metrics
        metrics = processor.get_metrics()

        print(f"✅ Processing completed!")
        print(f"📈 Total processed: {metrics.total_processed}")
        print(f"⚡ Processing rate: {metrics.processing_rate_per_second:.1f}/sec")
        print(f"🕐 Avg processing time: {metrics.avg_processing_time_ms:.1f}ms")
        print(f"❌ Error rate: {metrics.error_rate:.1%}")
        print(f"📦 Final backlog: {metrics.backlog_size}")
        print(f"🚦 Rate limit hits: {metrics.rate_limit_hits}")

        # Verify performance requirements
        success = True
        if metrics.processing_rate_per_second < 5:
            print("⚠️  Processing rate too low (< 5/sec)")
            success = False
        if metrics.error_rate > 0.3:
            print("⚠️  Error rate too high (> 30%)")
            success = False
        if metrics.backlog_size > 100:
            print("⚠️  Backlog too large (> 100 items)")
            success = False

        if success:
            print("🎉 All performance requirements met!")
        else:
            print("❌ Performance requirements not met")

        return success

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def print_test_header():
    """Print test header information"""
    print("=" * 60)
    print("REAL-TIME STREAM PROCESSING TEST")
    print("=" * 60)
    print("📝 Implement the StreamProcessor class")
    print("🎯 Handle concurrent streams, rate limits, and failures")
    print("⏱️  Focus on real-time performance and reliability\n")


def print_test_footer(success: bool):
    """Print test completion message"""
    if success:
        print("\n🎉 Implementation successful!")
        print("💡 Consider running additional stress tests")
    else:
        print("\n❌ Implementation needs improvement")
        print("💡 Check error handling, concurrency, and rate limiting")

    print("\n" + "=" * 60)
