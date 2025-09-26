# pdfmap_project/consumers/users.py
"""
User WebSocket consumer for user-specific updates
"""
import json
import logging
from .base import BaseWebSocketConsumer

logger = logging.getLogger(__name__)


class UserConsumer(BaseWebSocketConsumer):
    """WebSocket consumer for user-specific updates"""

    def get_group_prefix(self):
        return 'users'

    def get_consumer_name(self):
        return 'user updates'

    async def send_hello_message(self):
        """Send user-specific hello message"""
        await self.send(text_data=json.dumps({
            'type': 'user_hello',
            'message': f'Hello {self.user.username}! Connected to user updates.',
            'user_id': self.user.id,
            'username': self.user.username,
            'email': self.user.email,
            'timestamp': self.get_timestamp(),
        }))

    async def handle_custom_message(self, data):
        """Handle custom messages for user updates"""
        message_type = data.get('type', 'unknown')
        
        if message_type == 'get_user_info':
            await self.send_user_info()
        elif message_type == 'ping':
            await self.send(text_data=json.dumps({
                'type': 'pong',
                'message': 'pong',
                'timestamp': self.get_timestamp()
            }))
        else:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Unknown message type: {message_type}',
                'timestamp': self.get_timestamp()
            }))

    async def send_user_info(self):
        """Send current user information"""
        await self.send(text_data=json.dumps({
            'type': 'user_info',
            'user_id': self.user.id,
            'username': self.user.username,
            'email': self.user.email,
            'first_name': self.user.first_name,
            'last_name': self.user.last_name,
            'is_active': self.user.is_active,
            'is_staff': self.user.is_staff,
            'date_joined': self.user.date_joined.isoformat() if self.user.date_joined else None,
            'last_login': self.user.last_login.isoformat() if self.user.last_login else None,
            'timestamp': self.get_timestamp()
        }))

    async def user_message(self, event):
        """Handle user-specific messages from groups"""
        try:
            logger.debug(f"Received user message: {event}")
            
            # Send user-specific message to client
            await self.send(text_data=json.dumps({
                'type': 'user_message',
                'data': event,
                'timestamp': self.get_timestamp()
            }))

            logger.debug(f"User message delivered to user {self.user.id}")

        except Exception as e:
            logger.error(f"Failed to handle user message: {e}")

    async def user_notification(self, event):
        """Handle user notifications"""
        try:
            logger.debug(f"Received user notification: {event}")
            
            # Send notification to client
            await self.send(text_data=json.dumps({
                'type': 'user_notification',
                'notification': event,
                'timestamp': self.get_timestamp()
            }))

            logger.debug(f"User notification delivered to user {self.user.id}")

        except Exception as e:
            logger.error(f"Failed to handle user notification: {e}")

    async def user_status_update(self, event):
        """Handle user status updates"""
        try:
            logger.debug(f"Received user status update: {event}")
            
            # Send status update to client
            await self.send(text_data=json.dumps({
                'type': 'user_status_update',
                'status': event,
                'timestamp': self.get_timestamp()
            }))

            logger.debug(f"User status update delivered to user {self.user.id}")

        except Exception as e:
            logger.error(f"Failed to handle user status update: {e}")

    async def event_message(self, event):
        """Handle event messages (forwarded from events consumer)"""
        try:
            logger.debug(f"Received event message in user consumer: {event}")
            
            # Forward event message to client
            await self.send(text_data=json.dumps({
                'type': 'event_message',
                'event': event,
                'timestamp': self.get_timestamp()
            }))

            logger.debug(f"Event message forwarded to user {self.user.id}")

        except Exception as e:
            logger.error(f"Failed to handle event message: {e}")
