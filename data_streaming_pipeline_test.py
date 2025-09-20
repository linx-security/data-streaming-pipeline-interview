"""
SCENARIO:
You need to build a real-time data ingestion pipeline that processes incoming
data streams from multiple sources, validates the data, applies transformations,
and delivers results to downstream systems while handling rate limits and failures.

PIPELINE FLOW:
1. Consume data from the provided data_stream
2. Transform each StreamData item using transform_data()
3. Send transformed ProcessedRecord to downstream_api.send_data()
4. Track metrics throughout the entire pipeline process
5. Handle rate limits and failures gracefully with retries

REQUIREMENTS:
1. Process data from multiple concurrent sources
2. Respect rate limits for downstream APIs
3. Handle various failure scenarios gracefully
4. Provide real-time processing metrics
5. Ensure data integrity and ordering

Implement the StreamProcessor class below to pass all requirements.
"""

import asyncio
from typing import AsyncGenerator

# Import the test framework - DO NOT MODIFY THIS IMPORT
from data_streaming_pipeline_interview.candidate.streaming_test_framework import (
    DownstreamAPI,
    PipelineMetrics,
    ProcessedRecord,
    StreamData,
    print_test_footer,
    print_test_header,
    run_stream_test,
)

# =============================================================================
# YOUR IMPLEMENTATION
# =============================================================================


class StreamProcessor:
    """
    Real-time data stream processor with rate limiting and error recovery.

    TODO: Implement this class to handle concurrent stream processing.
    """

    def __init__(self, downstream_api: DownstreamAPI, max_concurrent_streams: int = 5):
        """
        Initialize the stream processor.

        Args:
            downstream_api: API client for sending processed data
            max_concurrent_streams: Maximum number of concurrent processing streams
        """
        # TODO: Initialize your processor
        pass

    async def start_processing(self, data_stream: AsyncGenerator[StreamData, None]) -> None:
        """
        Start processing the incoming data stream.

        This method should:
        1. Consume StreamData objects from data_stream
        2. Transform each item using transform_data()
        3. Send ProcessedRecord to downstream_api.send_data()
        4. Update metrics throughout the process

        Args:
            data_stream: Async generator yielding StreamData objects
        """
        # TODO: Implement stream processing
        pass

    async def transform_data(self, stream_data: StreamData) -> ProcessedRecord:
        """
        Transform raw stream data into processed record for downstream delivery.

        The returned ProcessedRecord should be sent to downstream_api.send_data().

        Args:
            stream_data: Raw data from stream

        Returns:
            ProcessedRecord: Validated and transformed data ready for downstream API
        """
        # TODO: Implement data transformation and validation
        pass

    def get_metrics(self) -> PipelineMetrics:
        """
        Get current pipeline processing metrics.

        Should track: processed count, error rates, processing times,
        rate limit hits, and source/priority breakdowns.

        Returns:
            PipelineMetrics: Current processing statistics
        """
        # TODO: Return current metrics
        pass

    async def stop_processing(self) -> None:
        """Stop processing and cleanup resources."""
        # TODO: Implement graceful shutdown
        pass


# =============================================================================
# TEST RUNNER
# =============================================================================


async def main():
    """Main test function"""
    print_test_header()

    success = await run_stream_test(StreamProcessor)

    print_test_footer(success)


if __name__ == "__main__":
    asyncio.run(main())
