"""
Data Models for Stream Processing Pipeline - CANDIDATE EDITABLE

This file contains the data models for your stream processing implementation.
You may modify this file as needed to ensure proper data validation and quality.

REQUIREMENTS: Your implementation must handle:
- Empty input data scenarios
- Data quality validation
- Timing edge cases and validation
- Processing time validation
- Priority value validation (1-4 range)
- Robust error handling

Choose the best approach for implementing validation to ensure data integrity.
"""

from typing import Any, Dict
from pydantic import BaseModel

class ProcessedRecord(BaseModel):
    """
    Validated and transformed data output from stream processing.

    This model represents the final output after your transform_data method
    has processed and validated the input stream data.

    Add proper validation to ensure data quality and handle edge cases.
    See CANDIDATE_README.md for specific validation requirements.
    """

    source: str
    timestamp: float
    priority: int
    processed_at: float
    transformed_data: Dict[str, Any]
    processing_time_ms: float