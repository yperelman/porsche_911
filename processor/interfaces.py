"""
Interface definitions for the stream processor components.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional


@dataclass
class Record:
    """Represents a record from a Kafka topic."""
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: Dict[str, Any]


@dataclass
class OutputRecord:
    """Represents an output record with attribution."""
    page_view_id: str
    user_id: str
    event_time: str
    url: str
    attributed_campaign_id: Optional[str]
    attributed_click_id: Optional[str]


class Source(ABC):
    """
    Interface for consuming records from input streams.

    Implementations should:
    - Provide records from one or more partitions
    - Include offset metadata for each record
    - Support checkpoint/restore of offsets
    """

    @abstractmethod
    def consume(self) -> Iterator[Record]:
        """
        Consume records from the source.

        Yields:
            Record: Next record from the source with offset metadata
        """
        pass

    @abstractmethod
    def commit_offsets(self, offsets: Dict[tuple, int]) -> None:
        """
        Commit offsets for consumed records.

        Args:
            offsets: Dict mapping (topic, partition) to offset
        """
        pass


class Sink(ABC):
    """
    Interface for writing output records.

    Implementations should:
    - Support durable writes (flush to disk/DB)
    - Provide idempotency (handle duplicates gracefully)
    - Be safe for concurrent writes if needed
    """

    @abstractmethod
    def write(self, record: OutputRecord) -> None:
        """
        Write an output record.

        Args:
            record: The output record to write
        """
        pass

    @abstractmethod
    def flush(self) -> None:
        """
        Ensure all written records are durable.
        This must complete before offsets can be committed.
        """
        pass


class OffsetStore(ABC):
    """
    Interface for persisting consumer offsets.

    Implementations should:
    - Provide atomic read-modify-write of offsets
    - Be crash-safe (survive process restart)
    - Support per-partition offset tracking
    """

    @abstractmethod
    def load_offsets(self) -> Dict[tuple, int]:
        """
        Load committed offsets.

        Returns:
            Dict mapping (topic, partition) to offset
        """
        pass

    @abstractmethod
    def save_offsets(self, offsets: Dict[tuple, int]) -> None:
        """
        Persist offsets atomically.

        Args:
            offsets: Dict mapping (topic, partition) to offset
        """
        pass
