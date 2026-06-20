"""
Main entry point for the stream processor.

TODO: Implement the orchestration logic that ties everything together.
"""

import signal
import sys
from typing import Dict

from processor.consumer import MultiTopicConsumer
from processor.join_engine import WindowedJoinEngine
from processor.output import FileSink


class StreamProcessor:
    """
    Main stream processor orchestrator.

    TODO: Implement the main processing loop.

    Responsibilities:
    - Create consumer, join engine, and sink
    - Coordinate processing of records
    - Handle offset commits safely
    - Support graceful shutdown
    - Optional: concurrent processing per partition
    """

    def __init__(
        self,
        bootstrap_servers: str = 'kafka:29092',
        output_path: str = 'output/attributed_page_views.jsonl',
        window_minutes: int = 30,
        lateness_minutes: int = 2,
        num_workers: int = 1
    ):
        """
        Initialize stream processor.

        Args:
            bootstrap_servers: Kafka broker address
            output_path: Output file path
            window_minutes: Attribution window size
            lateness_minutes: Allowed lateness
            num_workers: Number of concurrent workers
        """
        self.bootstrap_servers = bootstrap_servers
        self.output_path = output_path
        self.num_workers = num_workers

        # Initialize components
        # TODO: Create consumer, join engine, sink
        self.running = False
        pass

    def run(self) -> None:
        """
        Main processing loop.

        TODO: Implement processing loop
        - Consume records from both topics
        - Route clicks to join engine
        - Route page views to join engine
        - Write outputs to sink
        - Flush sink
        - Commit offsets
        - Handle errors and retries
        """
        pass

    def shutdown(self) -> None:
        """
        Graceful shutdown.

        TODO: Implement cleanup
        - Stop consuming
        - Flush pending outputs
        - Commit final offsets
        - Close resources
        """
        pass


def main():
    """
    Entry point for the processor.

    TODO: Setup signal handlers and start processor
    """
    processor = StreamProcessor()

    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print("\n⚠ Shutdown signal received. Cleaning up...")
        processor.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("🚀 Starting stream processor...")
    print("=" * 80)
    processor.run()


if __name__ == '__main__':
    main()
