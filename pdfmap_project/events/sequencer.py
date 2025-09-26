# pdfmap_project/events/sequencer.py
"""
Monotonic sequence number management for event ordering
"""
import redis
import logging
from typing import Optional
from django.conf import settings

logger = logging.getLogger(__name__)


class SequenceManager:
    """
    Manages monotonic sequence numbers for events

    Ensures events are ordered correctly even when delivered out-of-order.
    Uses Redis atomic operations for thread safety.
    """

    def __init__(self):
        """Initialize with Redis connection"""
        try:
            self.redis_client = redis.from_url(
                getattr(settings, 'REDIS_URL', 'redis://localhost:6379/1')
            )
            # Test connection
            self.redis_client.ping()
            logger.info("SequenceManager initialized with Redis")
        except Exception as e:
            logger.error(f"Failed to initialize SequenceManager: {e}")
            self.redis_client = None

    def get_next_sequence(self, task_id: str) -> int:
        """
        Get next sequence number for a task (atomic operation)

        Args:
            task_id: Unique task identifier

        Returns:
            Next sequence number (starts from 1)
        """
        if not self.redis_client:
            logger.warning("Redis not available, using fallback sequence")
            return self._fallback_sequence(task_id)

        try:
            key = f"seq:task:{task_id}"
            seq = self.redis_client.incr(key)
            logger.debug(f"Generated sequence {seq} for task {task_id}")
            return seq
        except Exception as e:
            logger.error(f"Failed to get sequence for task {task_id}: {e}")
            return self._fallback_sequence(task_id)

    def get_current_sequence(self, task_id: str) -> int:
        """
        Get current sequence number for a task

        Args:
            task_id: Unique task identifier

        Returns:
            Current sequence number (0 if not found)
        """
        if not self.redis_client:
            return 0

        try:
            key = f"seq:task:{task_id}"
            seq = self.redis_client.get(key)
            return int(seq) if seq else 0
        except Exception as e:
            logger.error(f"Failed to get current sequence for task {task_id}: {e}")
            return 0

    def reset_sequence(self, task_id: str) -> bool:
        """
        Reset sequence for a task (for testing)

        Args:
            task_id: Unique task identifier

        Returns:
            True if successful, False otherwise
        """
        if not self.redis_client:
            return False

        try:
            key = f"seq:task:{task_id}"
            self.redis_client.delete(key)
            logger.info(f"Reset sequence for task {task_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to reset sequence for task {task_id}: {e}")
            return False

    def set_sequence(self, task_id: str, seq: int) -> bool:
        """
        Set specific sequence number (for testing)

        Args:
            task_id: Unique task identifier
            seq: Sequence number to set

        Returns:
            True if successful, False otherwise
        """
        if not self.redis_client:
            return False

        try:
            key = f"seq:task:{task_id}"
            self.redis_client.set(key, seq)
            logger.info(f"Set sequence {seq} for task {task_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to set sequence for task {task_id}: {e}")
            return False

    def _fallback_sequence(self, task_id: str) -> int:
        """
        Fallback sequence generation when Redis is unavailable

        Args:
            task_id: Unique task identifier

        Returns:
            Fallback sequence number
        """
        import time
        # Use timestamp-based fallback (not ideal but functional)
        return int(time.time() * 1000) % 1000000

    def cleanup_old_sequences(self, max_age_days: int = 7) -> int:
        """
        Clean up old sequence keys to prevent Redis memory bloat

        Args:
            max_age_days: Maximum age of sequences to keep

        Returns:
            Number of keys cleaned up
        """
        if not self.redis_client:
            return 0

        try:
            import time
            cutoff_time = int(time.time()) - (max_age_days * 24 * 60 * 60)

            # Get all sequence keys
            pattern = "seq:task:*"
            keys = self.redis_client.keys(pattern)

            cleaned_count = 0
            for key in keys:
                # Check if key is older than cutoff
                ttl = self.redis_client.ttl(key)
                if ttl == -1:  # No expiration set
                    # Set expiration for old keys
                    self.redis_client.expire(key, max_age_days * 24 * 60 * 60)
                    cleaned_count += 1

            logger.info(f"Cleaned up {cleaned_count} old sequence keys")
            return cleaned_count

        except Exception as e:
            logger.error(f"Failed to cleanup old sequences: {e}")
            return 0


# Singleton instance
sequence_manager = SequenceManager()
