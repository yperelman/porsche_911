"""
State management for the windowed join.

This module should handle:
- Storing recent ad_click events per user
- Evicting old events to bound memory
- Thread-safe access for concurrent processing
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class ClickEvent:
    """Represents an ad click event."""
    user_id: str
    event_time: datetime
    campaign_id: str
    click_id: str


class JoinState:
    """
    Manages state for the windowed join.

    TODO: Implement state storage and eviction logic.

    Consider:
    - How to store clicks per user efficiently
    - How to evict old clicks (older than window + lateness)
    - Thread safety if processing concurrently
    - Memory bounds
    """

    def __init__(self, window_minutes: int = 30, lateness_minutes: int = 2):
        """
        Initialize join state.

        Args:
            window_minutes: Attribution window size in minutes
            lateness_minutes: Allowed lateness in minutes
        """
        self.window_minutes = window_minutes
        self.lateness_minutes = lateness_minutes
        # TODO: Add your state storage here
        pass

    def add_click(self, click: ClickEvent) -> None:
        """
        Add a click event to state.

        Args:
            click: The click event to store

        TODO: Implement storage logic
        """
        pass

    def get_latest_click(self, user_id: str, page_view_time: datetime) -> Optional[ClickEvent]:
        """
        Get the latest click for a user within the attribution window.

        Args:
            user_id: The user ID to look up
            page_view_time: The page view event time

        Returns:
            The latest click within the window, or None

        TODO: Implement lookup and window filtering
        """
        pass

    def evict_old_clicks(self, current_watermark: datetime) -> int:
        """
        Remove clicks older than the watermark.

        Args:
            current_watermark: Events older than this can be evicted

        Returns:
            Number of clicks evicted

        TODO: Implement eviction logic
        """
        pass
