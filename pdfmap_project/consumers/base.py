# pdfmap_project/consumers/base.py
"""
Base consumer with common functionality
"""
import json
import logging
import time
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth.models import AnonymousUser
from django.contrib.auth import get_user_model
from rest_framework_simplejwt.tokens import AccessToken
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError

logger = logging.getLogger(__name__)
User = get_user_model()


class BaseWebSocketConsumer(AsyncWebsocketConsumer):
    """Base WebSocket consumer with common functionality"""

    async def connect(self):
        """Handle WebSocket connection"""
        self.user = None
        self.user_group_name = None
        # Authenticate user
        user = await self.authenticate_user()
        if not user or isinstance(user, AnonymousUser):
            logger.warning(
                "WebSocket authentication failed for scope %s (query=%s)",
                self.scope.get('path'),
                self.scope.get('query_string'),
            )
            await self.close(code=4403)  # Unauthorized per spec
            return

        self.user = user
        self.user_group_name = f"user_{user.id}"

        # Join user group
        await self.channel_layer.group_add(
            self.user_group_name,
            self.channel_name
        )

        await self.accept()

        # Send hello message
        await self.send_hello_message()

        logger.info(f"User {user.username} connected to {self.get_consumer_name()}")

    async def disconnect(self, close_code):
        """Handle WebSocket disconnection"""
        if self.user_group_name:
            await self.channel_layer.group_discard(
                self.user_group_name,
                self.channel_name
            )
        logger.info(f"User {self.user.username if self.user else 'Anonymous'} disconnected from {self.get_consumer_name()}")

    async def receive(self, text_data):
        """Handle messages from WebSocket client"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type', 'unknown')

            if message_type == 'ping':
                await self.send(text_data=json.dumps({
                    'type': 'pong',
                    'message': 'pong',
                    'timestamp': self.get_timestamp()
                }))
            else:
                await self.handle_custom_message(data)

        except json.JSONDecodeError:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': 'Invalid JSON',
                'timestamp': self.get_timestamp()
            }))

    async def handle_custom_message(self, data):
        """Handle custom messages - override in subclasses"""
        await self.send(text_data=json.dumps({
            'type': 'error',
            'message': f'Unknown message type: {data.get("type", "unknown")}',
            'timestamp': self.get_timestamp()
        }))

    async def send_hello_message(self):
        """Send hello message - override in subclasses"""
        await self.send(text_data=json.dumps({
            'type': 'hello',
            'message': f'Hello {self.user.username}! Connected to {self.get_consumer_name()}.',
            'user_id': self.user.id,
            'timestamp': self.get_timestamp()
        }))

    def get_group_prefix(self):
        """Get group prefix - override in subclasses"""
        return 'user'

    def get_consumer_name(self):
        """Get consumer name - override in subclasses"""
        return 'WebSocket'

    @database_sync_to_async
    def authenticate_user(self):
        """Authenticate user from JWT token or session"""
        token = self.get_jwt_token()
        if token:
            try:
                access_token = AccessToken(token)
                user_id = access_token['user_id']
                return User.objects.get(id=user_id)
            except (InvalidToken, TokenError) as exc:
                logger.warning("Invalid JWT token: %s", exc)
            except User.DoesNotExist:
                logger.warning("JWT token user_id not found: %s", token)

        if hasattr(self.scope, 'user') and self.scope['user'].is_authenticated:
            return self.scope['user']

        return AnonymousUser()

    def get_jwt_token(self):
        """Extract JWT token from query parameters or headers"""
        query_string = self.scope.get('query_string', b'').decode()
        if 'token=' in query_string:
            for param in query_string.split('&'):
                if param.startswith('token='):
                    return param.split('=', 1)[1]

        headers = dict(self.scope.get('headers', []))
        auth_header = headers.get(b'authorization', b'').decode()
        if auth_header.startswith('Bearer '):
            return auth_header[7:]

        return None

    def get_timestamp(self):
        """Get current timestamp"""
        return int(time.time() * 1000)
