"""
Watermark tracking for handling out-of-order events.

A watermark represents the event time up to which we believe all events have arrived.
Events with event_time < watermark are considered late.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional


class WatermarkTracker:
    """
    Tracks watermarks for determining when to emit results.

    TODO: Implement watermark logic.

    Key concepts:
    - Watermark = max observed event_time - allowed lateness
    - Events arriving with event_time < watermark are late
    - Can emit results once watermark passes the window end
    """

    def __init__(self, lateness_minutes: int = 2):
        """
        Initialize watermark tracker.

        Args:
            lateness_minutes: How late events can arrive
        """
        self.lateness = timedelta(minutes=lateness_minutes)
        self.max_event_time: Optional[datetime] = None
        # TODO: Track per-partition watermarks if needed
        pass

    def update(self, event_time: datetime) -> None:
        """
        Update watermark with a new event timestamp.

        Args:
            event_time: The event time from the latest record

        TODO: Implement watermark update logic
        """
        pass

    def get_watermark(self) -> Optional[datetime]:
        """
        Get the current watermark.

        Returns:
            Current watermark timestamp, or None if not yet set

        TODO: Calculate watermark based on max event time and lateness
        """
        pass

    def is_late(self, event_time: datetime) -> bool:
        """
        Check if an event is late (beyond allowed lateness).

        Args:
            event_time: The event time to check

        Returns:
            True if event is late, False otherwise

        TODO: Compare event time against watermark
        """
        pass
