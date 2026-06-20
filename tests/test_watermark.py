"""
Tests for watermark tracking.

TODO: Implement tests for watermark logic.
"""

import pytest
from datetime import datetime, timedelta

from processor.watermark import WatermarkTracker


class TestWatermarkTracker:
    """Test watermark tracking behavior."""

    def test_initial_watermark_is_none(self):
        """Watermark should be None before any events."""
        # TODO: Implement test
        pass

    def test_watermark_updates_with_events(self):
        """Watermark should update as events arrive."""
        # TODO: Implement test
        # Test that watermark = max_event_time - lateness
        pass

    def test_late_event_detection(self):
        """Events before watermark should be marked as late."""
        # TODO: Implement test
        pass

    def test_watermark_monotonically_increases(self):
        """Watermark should never go backwards."""
        # TODO: Implement test
        # Even if an out-of-order event arrives, watermark shouldn't decrease
        pass
