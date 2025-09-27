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
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

from pydantic import BaseModel

# Import ProcessedRecord from candidate models
from data_streaming_pipeline_interview.candidate.models import ProcessedRecord

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


# ProcessedRecord moved to candidate/models.py for candidate implementation


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


@dataclass
class FrameworkMetrics:
    """Framework-tracked metrics for validation"""
    total_api_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rate_limit_hits: int = 0
    call_timestamps: list = None
    start_time: float = None

    def __post_init__(self):
        if self.call_timestamps is None:
            self.call_timestamps = []
        if self.start_time is None:
            self.start_time = time.time()


class DownstreamAPI:
    """Simulated downstream API with rate limiting and failures"""

    def __init__(self, max_requests_per_second: int = 10, failure_rate: float = 0.1):
        self.max_requests_per_second = max_requests_per_second
        self.failure_rate = failure_rate
        self.request_timestamps = []
        self.total_requests = 0

        # Framework metrics tracking
        self.framework_metrics = FrameworkMetrics()

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

        # Framework metrics tracking - record attempt
        self.framework_metrics.total_api_calls += 1

        # Remove old timestamps (older than 1 second)
        self.request_timestamps = [ts for ts in self.request_timestamps if current_time - ts < 1.0]

        # Check rate limit
        if len(self.request_timestamps) >= self.max_requests_per_second:
            self.framework_metrics.rate_limit_hits += 1
            raise RateLimitError("Rate limit exceeded")

        # Simulate network delay
        await asyncio.sleep(random.uniform(0.01, 0.05))

        # Simulate random failures
        if random.random() < self.failure_rate:
            self.framework_metrics.failed_calls += 1
            raise ProcessingError("Downstream API failure")

        # Success
        self.request_timestamps.append(current_time)
        self.framework_metrics.successful_calls += 1
        self.framework_metrics.call_timestamps.append(current_time)
        return True


# =============================================================================
# DATA STREAM SIMULATOR
# =============================================================================


class DataStreamSimulator:
    """Simulates real-time data streams from multiple sources with occasional data quality issues"""

    def __init__(self, sources: List[DataSource], rate_per_source: int = 5, bad_data_rate: float = 0.15):
        self.sources = sources
        self.rate_per_source = rate_per_source
        self.bad_data_rate = bad_data_rate  # 15% chance of problematic data
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

                    # Generate potentially problematic data to test validation
                    should_generate_bad_data = random.random() < self.bad_data_rate

                    yield StreamData(
                        source=source,
                        timestamp=time.time(),
                        priority=random.choice(list(Priority)),
                        data=self._generate_sample_data(source, should_generate_bad_data),
                        sequence_id=self.sequence_counters[source],
                    )

            # Control the rate
            await asyncio.sleep(1.0 / (self.rate_per_source * len(self.sources)))

    def stop(self):
        """Stop the data stream"""
        self.is_running = False

    def _generate_sample_data(self, source: DataSource, generate_bad_data: bool = False) -> Dict[str, Any]:
        """Generate realistic sample data for each source type, occasionally with data quality issues"""

        # Generate bad data occasionally to test validation logic
        if generate_bad_data:
            return self._generate_problematic_data(source)

        # Generate normal, well-formed data
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

    def _generate_problematic_data(self, source: DataSource) -> Dict[str, Any]:
        """
        Generate data designed to test ProcessedRecord validation logic.

        This creates edge cases that challenge how candidates handle:
        - Empty input data (should produce valid ProcessedRecord with meaningful defaults)
        - Unusual but valid input data (should be handled gracefully)
        - Data that might cause timing, processing, or validation edge cases
        """

        # Types of edge cases to test ProcessedRecord validation
        edge_case_types = [
            "empty_input",           # Test empty data handling
            "minimal_data",          # Test minimal valid data
            "timing_edge_case",      # Test timing-related challenges
            "large_data",           # Test handling of large data volumes
            "special_characters"     # Test special character handling
        ]

        edge_case = random.choice(edge_case_types)

        if edge_case == "empty_input":
            # Empty input - candidate must produce meaningful ProcessedRecord
            return {}

        elif edge_case == "minimal_data":
            # Minimal but valid data - tests default handling
            if source == DataSource.USER_EVENTS:
                return {"event": "minimal"}
            elif source == DataSource.TRANSACTION_DATA:
                return {"type": "test"}
            elif source == DataSource.SYSTEM_METRICS:
                return {"status": "ok"}
            else:  # SECURITY_LOGS
                return {"event": "test"}

        elif edge_case == "timing_edge_case":
            # Data that might cause timing-related validation challenges
            # This tests if candidates handle timing edge cases properly
            if source == DataSource.USER_EVENTS:
                return {
                    "user_id": "timing_test_user",
                    "event_type": "rapid_fire",
                    "timestamp_hint": "edge_case",  # Unusual field that might affect timing
                }
            else:
                return {
                    "test_type": "timing_challenge",
                    "rapid_processing": True,
                }

        elif edge_case == "large_data":
            # Large data volume to test processing time validation
            large_data = {f"field_{i}": f"value_{i}" for i in range(50)}
            large_data["data_type"] = "large_volume_test"
            return large_data

        else:  # special_characters
            # Data with special characters that should be handled gracefully
            if source == DataSource.USER_EVENTS:
                return {
                    "user_id": "user_with_special_chars_测试",
                    "event_type": "special_event",
                    "description": "Event with émojis 🎉 and unicode ñ",
                }
            elif source == DataSource.TRANSACTION_DATA:
                return {
                    "transaction_id": "txn_special_chars_αβγ",
                    "description": "Transaction with special chars: €£¥",
                    "metadata": {"unicode_test": "café naïve résumé"},
                }
            else:
                return {
                    "description": "Log entry with special characters: ©®™",
                    "unicode_field": "Testing unicode: 中文 العربية русский",
                }


# =============================================================================
# VALIDATION TEST DATA GENERATOR
# =============================================================================


def generate_test_processed_records():
    """
    Generate test data specifically for ProcessedRecord validation testing.
    Returns list of (record_data, should_pass, reason) tuples.
    """
    current_time = time.time()

    test_cases = [
        # Valid records
        ({
            "source": "user_events",
            "timestamp": current_time,
            "priority": 2,
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": 50.0
        }, True, "valid_record"),

        # Priority validation tests
        ({
            "source": "user_events",
            "timestamp": current_time,
            "priority": 0,  # Invalid - below range
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": 50.0
        }, False, "priority_below_range"),

        ({
            "source": "user_events",
            "timestamp": current_time,
            "priority": 5,  # Invalid - above range
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": 50.0
        }, False, "priority_above_range"),

        # Processing time validation tests
        ({
            "source": "transaction_data",
            "timestamp": current_time,
            "priority": 2,
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": -10.0  # Invalid - negative
        }, False, "negative_processing_time"),

        ({
            "source": "system_metrics",
            "timestamp": current_time,
            "priority": 3,
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": 15000.0  # Invalid - too high (15 seconds)
        }, False, "excessive_processing_time"),

        # Timestamp validation tests
        ({
            "source": "security_logs",
            "timestamp": current_time,
            "priority": 4,
            "processed_at": current_time - 5,  # Invalid - processed before received
            "transformed_data": {"test": "data"},
            "processing_time_ms": 50.0
        }, False, "processed_before_timestamp"),

        # Empty/invalid data tests
        ({
            "source": "user_events",
            "timestamp": current_time,
            "priority": 2,
            "processed_at": current_time + 1,
            "transformed_data": {},  # Invalid - empty
            "processing_time_ms": 50.0
        }, False, "empty_transformed_data"),

        ({
            "source": "",  # Invalid - empty source
            "timestamp": current_time,
            "priority": 2,
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": 50.0
        }, False, "empty_source"),

        # Source validation tests
        ({
            "source": "invalid_source",  # Invalid - not in DataSource enum
            "timestamp": current_time,
            "priority": 2,
            "processed_at": current_time + 1,
            "transformed_data": {"test": "data"},
            "processing_time_ms": 50.0
        }, False, "invalid_source_enum"),

        # Edge case tests
        ({
            "source": "user_events",
            "timestamp": current_time,
            "priority": 1,
            "processed_at": current_time,  # Edge case - same time
            "transformed_data": {"minimal": "data"},
            "processing_time_ms": 0.0  # Edge case - zero processing time
        }, True, "edge_case_minimal_valid"),

        ({
            "source": "transaction_data",
            "timestamp": current_time,
            "priority": 4,
            "processed_at": current_time + 0.001,  # Very small difference
            "transformed_data": {"large_data": "x" * 1000},  # Large data
            "processing_time_ms": 9999.0  # Maximum reasonable processing time
        }, True, "edge_case_maximum_valid")
    ]

    return test_cases


# =============================================================================
# METRICS COMPARISON
# =============================================================================


def compare_metrics(candidate_metrics: PipelineMetrics, framework_metrics: FrameworkMetrics) -> Dict[str, bool]:
    """
    Compare candidate metrics against framework-tracked metrics.
    Returns dict of validation results with reasonable tolerances.
    """
    results = {}

    # Total processed comparison
    # Allow some tolerance since timing can affect this
    framework_total = framework_metrics.successful_calls
    candidate_total = candidate_metrics.total_processed

    # Allow 10% difference or ±2 items, whichever is more lenient
    tolerance = max(2, framework_total * 0.1)
    results['total_processed_accurate'] = abs(candidate_total - framework_total) <= tolerance

    # Rate limit hits comparison
    # Very lenient check - main goal is to ensure candidates are tracking rate limits at all
    # Different implementations may count at different levels (token bucket, API, retry logic)
    results['rate_limit_hits_accurate'] = (
        candidate_metrics.rate_limit_hits >= 0 and  # Must be non-negative
        candidate_metrics.rate_limit_hits <= 1000  # Reasonable upper bound
    )

    # Error rate comparison
    framework_error_rate = (
        framework_metrics.failed_calls / max(framework_metrics.total_api_calls, 1)
    )
    error_rate_diff = abs(candidate_metrics.error_rate - framework_error_rate)
    results['error_rate_accurate'] = error_rate_diff <= 0.15  # 15% tolerance

    # Processing rate comparison
    if len(framework_metrics.call_timestamps) >= 2:
        elapsed = max(framework_metrics.call_timestamps[-1] - framework_metrics.start_time, 0.1)
        framework_rate = framework_metrics.successful_calls / elapsed
        rate_diff = abs(candidate_metrics.processing_rate_per_second - framework_rate)
        rate_tolerance = max(1.0, framework_rate * 0.2)  # 20% tolerance
        results['processing_rate_accurate'] = rate_diff <= rate_tolerance
    else:
        results['processing_rate_accurate'] = True  # Skip if not enough data

    return results


def print_metrics_comparison(candidate_metrics: PipelineMetrics, framework_metrics: FrameworkMetrics, results: Dict[str, bool]):
    """Print detailed metrics comparison for debugging"""
    print("\n🔍 METRICS VALIDATION:")
    print("=" * 40)

    framework_total = framework_metrics.successful_calls
    print(f"Total Processed: Candidate={candidate_metrics.total_processed}, Framework={framework_total} {'✅' if results['total_processed_accurate'] else '❌'}")

    print(f"Rate Limit Hits: Candidate={candidate_metrics.rate_limit_hits}, Framework={framework_metrics.rate_limit_hits} {'✅' if results['rate_limit_hits_accurate'] else '❌'}")

    framework_error_rate = framework_metrics.failed_calls / max(framework_metrics.total_api_calls, 1)
    print(f"Error Rate: Candidate={candidate_metrics.error_rate:.3f}, Framework={framework_error_rate:.3f} {'✅' if results['error_rate_accurate'] else '❌'}")

    if len(framework_metrics.call_timestamps) >= 2:
        elapsed = max(framework_metrics.call_timestamps[-1] - framework_metrics.start_time, 0.1)
        framework_rate = framework_metrics.successful_calls / elapsed
        print(f"Processing Rate: Candidate={candidate_metrics.processing_rate_per_second:.1f}/sec, Framework={framework_rate:.1f}/sec {'✅' if results['processing_rate_accurate'] else '❌'}")

    accuracy_score = sum(results.values()) / len(results) * 100
    print(f"\n📊 Metrics Accuracy: {accuracy_score:.0f}%")


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

        # Validate metrics accuracy against framework tracking
        comparison_results = compare_metrics(metrics, downstream_api.framework_metrics)
        print_metrics_comparison(metrics, downstream_api.framework_metrics, comparison_results)

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

        # Check metrics accuracy (with some tolerance for timing differences)
        metrics_accuracy = sum(comparison_results.values()) / len(comparison_results)
        if metrics_accuracy < 0.75:  # 75% of metrics must be accurate
            print("⚠️  Metrics accuracy too low - check get_metrics() implementation")
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
