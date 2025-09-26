# pdfmap_project/consumers/events.py
"""
Event WebSocket consumer for lightweight event envelopes
"""
import json
import logging
import redis
from django.conf import settings
from .base import BaseWebSocketConsumer
from pdfmap_project.events.permissions import PermissionChecker

# Redis connection for tracking group memberships
redis_client = redis.Redis.from_url(getattr(settings, 'REDIS_URL', 'redis://localhost:6379/0'))

logger = logging.getLogger(__name__)
class EventConsumer(BaseWebSocketConsumer):
    """WebSocket consumer for lightweight event envelopes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_memberships = set()

    def get_group_prefix(self):
        return 'events'

    def get_consumer_name(self):
        return 'event updates'

    async def send_hello_message(self):
        """Send event-specific hello message"""
        await self.send(text_data=json.dumps({
            'type': 'event_hello',
            'message': f'Hello {self.user.username}! Connected to event updates.',
            'user_id': self.user.id,
            'timestamp': self.get_timestamp(),
            'supported_events': [
                'TASK_QUEUED',
                'TASK_STARTED',
                'TASK_PROGRESS',
                'TASK_COMPLETED',
                'TASK_FAILED',
                'NOTIFICATION'
            ]
        }))

    async def handle_custom_message(self, data):
        """Handle custom messages for events"""
        message_type = data.get('type', 'unknown')

        if message_type == 'ping':
            await self.send(text_data=json.dumps({
                'type': 'pong',
                'message': 'pong',
                'timestamp': self.get_timestamp()
            }))
        elif message_type == 'subscribe_events':
            event_types = data.get('event_types', [])
            await self._subscribe_to_event_types(event_types)

            # Optional: subscribe to explicit group names
            groups = data.get('groups', [])
            await self._subscribe_to_groups(groups)
        elif message_type == 'unsubscribe_events':
            event_types = data.get('event_types', [])
            await self._unsubscribe_from_event_types(event_types)

            groups = data.get('groups', [])
            await self._unsubscribe_from_groups(groups)
        elif message_type == 'get_event_stats':
            # Return event statistics
            await self._send_event_stats()
        else:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Unknown message type: {message_type}',
                'timestamp': self.get_timestamp()
            }))

    async def _subscribe_to_event_types(self, event_types):
        """Subscribe to specific event types"""
        try:
            # Join event-specific groups using valid naming convention
            for event_type in event_types:
                if event_type in ['TASK_QUEUED', 'TASK_STARTED', 'TASK_PROGRESS', 'TASK_COMPLETED', 'TASK_FAILED', 'NOTIFICATION']:
                    group_name = f"events_{event_type.lower()}"
                    if PermissionChecker.can_user_access_group(self.user.id, group_name):
                        await self.channel_layer.group_add(
                            group_name,
                            self.channel_name
                        )
                        logger.info("[WS] user %s joined group %s", self.user.id, group_name)

            await self.send(text_data=json.dumps({
                'type': 'events_subscribed',
                'message': f'Subscribed to {len(event_types)} event types',
                'event_types': event_types,
                'timestamp': self.get_timestamp()
            }))

        except Exception as e:
            logger.error(f"Failed to subscribe to events: {e}")
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Failed to subscribe to events: {str(e)}',
                'timestamp': self.get_timestamp()
            }))

    async def _unsubscribe_from_event_types(self, event_types):
        """Unsubscribe from specific event types"""
        try:
            for event_type in event_types:
                group_name = f"events_{event_type.lower()}"
                await self.channel_layer.group_discard(
                    group_name,
                    self.channel_name
                )

            await self.send(text_data=json.dumps({
                'type': 'events_unsubscribed',
                'message': f'Unsubscribed from {len(event_types)} event types',
                'event_types': event_types,
                'timestamp': self.get_timestamp()
            }))

        except Exception as e:
            logger.error(f"Failed to unsubscribe from events: {e}")
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Failed to unsubscribe from events: {str(e)}',
                'timestamp': self.get_timestamp()
            }))

    async def _subscribe_to_groups(self, groups):
        """
        Subscribe user to explicit group names (user/project/job/workspace).

        Normalizes group names to match backend publishing and logs all steps.
        """
        if not groups:
            logger.warning("[WS] No groups provided for subscription")
            return

        for raw_group in groups:
            # Normalize group name: replace colon with underscore
            group_name = raw_group.replace(':', '_').strip()
            if not group_name:
                continue

            # Log the incoming and normalized group
            logger.info("[WS] User %s subscribing to group: raw=%s, normalized=%s",
                        self.user.id, raw_group, group_name)

            # Optional: skip permission check temporarily for debugging
            # Remove or enable in production
            can_access = True
            try:
                can_access = PermissionChecker.can_user_access_group(self.user.id, group_name)
            except Exception as e:
                logger.warning("[WS] Permission check error for group %s: %s", group_name, e)

            if not can_access:
                logger.warning("[WS] User %s denied access to group %s", self.user.id, group_name)
                continue

            # Add to Channels group
            try:
                await self.channel_layer.group_add(group_name, self.channel_name)
                
                # Track membership in Redis
                redis_client.sadd(f"group_members:{group_name}", self.user.id)
                self.group_memberships.add(group_name)
                
                logger.info("[WS] User %s joined group %s", self.user.id, group_name)
            except Exception as e:
                logger.error("[WS] Failed to join group %s: %s", group_name, e)

    async def _unsubscribe_from_groups(self, groups):
        """Remove the socket from explicit groups and update membership"""
        for raw_group in groups or []:
            group_name = raw_group.replace(':', '_')
            await self.channel_layer.group_discard(group_name, self.channel_name)
            
            # Remove from Redis tracking
            redis_client.srem(f"group_members:{group_name}", self.user.id)
            self.group_memberships.discard(group_name)
            
            logger.info("[WS] User %s left group %s", self.user.id, group_name)

    async def _send_event_stats(self):
        """Send event statistics"""
        try:
            from pdfmap_project.events.publisher import event_publisher
            stats = event_publisher.get_event_stats()

            await self.send(text_data=json.dumps({
                'type': 'event_stats',
                'stats': stats,
                'timestamp': self.get_timestamp()
            }))

        except Exception as e:
            logger.error(f"Failed to get event stats: {e}")
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Failed to get event stats: {str(e)}',
                'timestamp': self.get_timestamp()
            }))

    async def event_message(self, event):
        """Handle event messages from groups"""
        try:
            logger.debug(f"Received event message: {event}")
            # The event data is directly in the event parameter, not nested under 'event' key
            event_data = event

            # Validate event envelope
            if not self._validate_event_envelope(event_data):
                logger.warning(f"Invalid event envelope received: {event_data}")
                return
            # Send event to client
            await self.send(text_data=json.dumps({
                'type': 'event',
                'event': event_data,
                'timestamp': self.get_timestamp()
            }))

            logger.debug(f"Event delivered to user {self.user.id}: {event_data.get('event_type')}")

        except Exception as e:
            logger.error(f"Failed to handle event message: {e}")


    def _validate_event_envelope(self, event_data):
        """Validate event envelope structure"""
        if not isinstance(event_data, dict):
            logger.warning(f"Event data is not a dictionary: {type(event_data)}")
            return False
            
        required_fields = [
            'event_type', 'task_id', 'job_type', 'project_id',
            'user_id', 'seq', 'ts', 'detail_url'
        ]

        for field in required_fields:
            if field not in event_data:
                logger.warning(f"Missing required field '{field}' in event data")
                return False

        # Validate event type
        if event_data['event_type'] not in [
            'TASK_QUEUED', 'TASK_STARTED', 'TASK_PROGRESS',
            'TASK_COMPLETED', 'TASK_FAILED', 'NOTIFICATION'
        ]:
            logger.warning(f"Invalid event type: {event_data['event_type']}")
            return False

        return True

    async def disconnect(self, close_code):
        """Clean up group memberships when user disconnects"""
        for group_name in self.group_memberships:
            redis_client.srem(f"group_members:{group_name}", self.user.id)
            logger.info("[WS] User %s removed from group %s on disconnect", self.user.id, group_name)
        
        await super().disconnect(close_code)
