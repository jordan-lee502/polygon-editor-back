# pdfmap_project/consumers/jobs.py
"""
Job WebSocket consumer
"""
import json
from .base import BaseWebSocketConsumer
import logging
import redis
from django.conf import settings

logger = logging.getLogger(__name__)

# Redis connection for tracking group memberships
try:
    redis_client = redis.Redis.from_url(getattr(settings, 'REDIS_URL', 'redis://localhost:6379/0'))
    redis_client.ping()  # Test connection
    print("Redis connected successfully for JobConsumer")
except Exception as e:
    print(f"Redis connection failed for JobConsumer: {e}")
    redis_client = None


class JobConsumer(BaseWebSocketConsumer):
    """WebSocket consumer for job status updates"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_memberships = set()

    def get_group_prefix(self):
        return 'jobs'

    def get_consumer_name(self):
        return 'job updates'

    async def send_hello_message(self):
        """Send job-specific hello message"""
        await self.send(text_data=json.dumps({
            'type': 'job_hello',
            'message': f'Hello {self.user.username}! Connected to job updates.',
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
        """Handle custom messages for jobs"""
        message_type = data.get('type', 'unknown')

        if message_type == 'subscribe_job':
            job_id = data.get('job_id')
            if job_id:
                # Subscribe to specific job updates
                job_group = f"job_{job_id}"
                await self.channel_layer.group_add(
                    job_group,
                    self.channel_name
                )
                await self.send(text_data=json.dumps({
                    'type': 'job_subscribed',
                    'job_id': job_id,
                    'message': f'Subscribed to job {job_id} updates',
                    'timestamp': self.get_timestamp()
                }))
            else:
                await self.send(text_data=json.dumps({
                    'type': 'error',
                    'message': 'Job ID required for subscription',
                    'timestamp': self.get_timestamp()
                }))
        elif message_type == 'process_page_region':
            await self.handle_process_page_region(data)
        elif message_type == 'ping':
            await self.send(text_data=json.dumps({
                'type': 'pong',
                'message': 'pong',
                'timestamp': self.get_timestamp()
            }))
        elif message_type == 'subscribe_jobs':
            # Subscribe to job groups
            groups = data.get('groups', [])
            print(f"[JOB_CONSUMER] received subscribe_jobs with groups: {groups}")
            print(f"[JOB_CONSUMER] User ID: {self.user.id}")
            await self._subscribe_to_groups(groups)
        elif message_type == 'unsubscribe_jobs':
            # Unsubscribe from job groups
            groups = data.get('groups', [])
            await self._unsubscribe_from_groups(groups)
        elif message_type == 'list_groups':
            # List all groups the user is currently subscribed to
            await self._list_user_groups()
        else:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Unknown message type: {message_type}',
                'timestamp': self.get_timestamp()
            }))

    async def handle_process_page_region(self, data):
        """Handle process_page_region requests"""
        try:
            from workspace.models import Workspace
            from processing.pdf_processor import process_page_region
            from django.core.exceptions import ObjectDoesNotExist
            from pdfmap_project.events.notifier import page_event
            from pdfmap_project.events.envelope import EventType, JobType
            
            workspace_id = data.get('workspace_id')
            page_number = data.get('page_number')
            rect_points = data.get('rect_points')
            segmentation_method = data.get('segmentation_method', 'GENERIC')
            dpi = data.get('dpi', 100)
            
            # Validate required parameters
            if not all([workspace_id, page_number, rect_points]):
                await self.send(text_data=json.dumps({
                    'type': 'error',
                    'message': 'Missing required parameters: workspace_id, page_number, rect_points',
                    'timestamp': self.get_timestamp()
                }))
                return
            
            # Get workspace
            try:
                workspace = Workspace.objects.get(pk=workspace_id)
            except ObjectDoesNotExist:
                await self.send(text_data=json.dumps({
                    'type': 'error',
                    'message': f'Workspace {workspace_id} not found',
                    'timestamp': self.get_timestamp()
                }))
                return
            
            # Get page extract status
            try:
                page = workspace.pages.get(page_number=page_number)
                extract_status = page.extract_status
                page_id = page.id
            except Exception:
                extract_status = None
                page_id = None
            
            # Publish processing started event to groups
            page_event(
                event_type=EventType.TASK_STARTED,
                task_id=str(workspace_id),
                project_id=str(workspace_id),
                user_id=workspace.user_id,
                job_type=JobType.POLYGON_EXTRACTION,
                page_id=page_id,
                page_number=page_number,
                workspace_id=str(workspace_id),
                payload={
                    "extract_status": extract_status,
                },
            )
            
            # Process the page region
            try:
                process_page_region(
                    ws=workspace,
                    page_number=page_number,
                    rect_points=rect_points,
                    segmentation_method=segmentation_method,
                    dpi=dpi
                )
                
                # Get updated page extract status after processing
                try:
                    page = workspace.pages.get(page_number=page_number)
                    updated_extract_status = page.extract_status
                    updated_page_id = page.id
                except Exception:
                    updated_extract_status = None
                    updated_page_id = None
                
                # Publish processing completed event to groups
                page_event(
                    event_type=EventType.TASK_COMPLETED,
                    task_id=str(workspace_id),
                    project_id=str(workspace_id),
                    user_id=workspace.user_id,
                    job_type=JobType.POLYGON_EXTRACTION,
                    page_id=updated_page_id,
                    page_number=page_number,
                    workspace_id=str(workspace_id),
                    payload={
                        "extract_status": updated_extract_status,
                    },
                )
                
            except Exception as e:
                # Get page extract status after failed processing
                try:
                    page = workspace.pages.get(page_number=page_number)
                    failed_extract_status = page.extract_status
                    failed_page_id = page.id
                except Exception:
                    failed_extract_status = None
                    failed_page_id = None
                
                # Publish processing failed event to groups
                page_event(
                    event_type=EventType.TASK_FAILED,
                    task_id=str(workspace_id),
                    project_id=str(workspace_id),
                    user_id=workspace.user_id,
                    job_type=JobType.POLYGON_EXTRACTION,
                    page_id=failed_page_id,
                    page_number=page_number,
                    workspace_id=str(workspace_id),
                    payload={
                        "extract_status": failed_extract_status,
                    },
                )
                
        except Exception as e:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Internal error: {str(e)}',
                'timestamp': self.get_timestamp()
            }))

    async def event_message(self, event):
        """Handle event messages from groups - similar to EventConsumer"""
        try:
            
            # The event data is directly in the event parameter, not nested under 'event' key
            event_data = event

            # Validate event envelope
            if not self._validate_event_envelope(event_data):
                logger.warning(f"Invalid event envelope received: {event_data}")
                print(f"Invalid event envelope received: {event_data}")
                return
                
            # Send event to client
            response = {
                'type': 'event',
                'event': event_data,
                'timestamp': self.get_timestamp()
            }
            await self.send(text_data=json.dumps(response))

            logger.debug(f"Event delivered to user {self.user.id}: {event_data.get('event_type')}")

        except Exception as e:
            logger.error(f"Failed to handle event message: {e}")
            print(f"Failed to handle event message: {e}")

    def _validate_event_envelope(self, event_data):
        """Validate event envelope structure - similar to EventConsumer"""
        
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

    async def _subscribe_to_groups(self, groups):
        """Subscribe to specific groups for job events"""
        
        print(f"[JOB_CONSUMER] _subscribe_to_groups called with groups: {groups}")
        print(f"[JOB_CONSUMER] Redis client available: {redis_client is not None}")
        
        if not groups:
            logger.warning("[WS] No groups provided for subscription")
            print("[WS] No groups provided for subscription")
            return

        for raw_group in groups:
            # Normalize group name: replace colon with underscore
            group_name = raw_group.replace(':', '_').strip()
            if not group_name:
                continue

            # Log the incoming and normalized group
            logger.info("[WS] User %s subscribing to group: raw=%s, normalized=%s",
                        self.user.id, raw_group, group_name)
            print(f"[WS] User {self.user.id} subscribing to group: raw={raw_group}, normalized={group_name}")

            # Add to Channels group
            try:
                await self.channel_layer.group_add(group_name, self.channel_name)
                logger.info("[WS] User %s joined group %s", self.user.id, group_name)
                print(f"[WS] User {self.user.id} joined group {group_name}")
                
                # Add to Redis group for notification tracking
                if redis_client:
                    try:
                        redis_key = f"group_members:{group_name}"
                        redis_client.sadd(redis_key, self.user.id)
                        print(f"[WS] Added user {self.user.id} to Redis group {group_name} (key: {redis_key})")
                        
                        # Verify the addition
                        members = redis_client.smembers(redis_key)
                        print(f"[WS] Redis group {group_name} now has members: {members}")
                        
                        # Also check if the key exists
                        exists = redis_client.exists(redis_key)
                        print(f"[WS] Redis key {redis_key} exists: {exists}")
                        
                    except Exception as redis_err:
                        print(f"[WS] Failed to add user to Redis group {group_name}: {redis_err}")
                else:
                    print(f"[WS] Redis client not available for group {group_name}")
                
                # Track group membership
                self.group_memberships.add(group_name)
                
                # Verify group membership
                await self._verify_group_membership(group_name)
            except Exception as e:
                logger.error("[WS] Failed to join group %s: %s", group_name, e)
                print(f"[WS] Failed to join group {group_name}: {e}")

        response = {
            'type': 'jobs_subscribed',
            'message': f'Subscribed to {len(groups)} groups',
            'groups': groups,
            'timestamp': self.get_timestamp()
        }
        print(f"JobConsumer sending response: {response}")
        await self.send(text_data=json.dumps(response))

    async def _verify_group_membership(self, group_name):
        """Verify that the user is actually a member of the group"""
        try:
            # Send a test message to the group to verify membership
            test_message = {
                'type': 'group_membership_test',
                'group_name': group_name,
                'user_id': self.user.id,
                'channel_name': self.channel_name,
                'timestamp': self.get_timestamp()
            }
            
            # Send the test message to the group
            await self.channel_layer.group_send(group_name, test_message)
            print(f"[WS] Sent group membership test message to {group_name}")
            
        except Exception as e:
            print(f"[WS] Failed to verify group membership for {group_name}: {e}")

    async def group_membership_test(self, event):
        """Handle group membership test messages"""
        print(f"[WS] Group membership test received for group {event.get('group_name')}")
        print(f"[WS] Test message: {event}")
        
        # Send confirmation back to the client
        await self.send(text_data=json.dumps({
            'type': 'group_membership_confirmed',
            'group_name': event.get('group_name'),
            'user_id': event.get('user_id'),
            'message': f'Successfully verified membership in group {event.get("group_name")}',
            'timestamp': self.get_timestamp()
        }))

    async def _list_user_groups(self):
        """List all groups the user is currently subscribed to"""
        try:
            # Get all groups from the channel layer
            # Note: This is a simplified approach - in production you might want to track this differently
            user_groups = []
            
            # Check common group patterns
            user_id = self.user.id
            common_groups = [
                f"user_{user_id}",
                f"project_{user_id}",  # if project_id equals user_id
                # Add more patterns as needed
            ]
            
            # Send test messages to check membership
            for group_name in common_groups:
                try:
                    test_message = {
                        'type': 'group_check',
                        'group_name': group_name,
                        'user_id': user_id,
                        'timestamp': self.get_timestamp()
                    }
                    await self.channel_layer.group_send(group_name, test_message)
                    user_groups.append(group_name)
                except:
                    pass  # Group doesn't exist or user not a member
            
            # Send the list back to the client
            await self.send(text_data=json.dumps({
                'type': 'user_groups_list',
                'groups': user_groups,
                'user_id': user_id,
                'message': f'User {user_id} is subscribed to {len(user_groups)} groups',
                'timestamp': self.get_timestamp()
            }))
            
            print(f"[WS] User {user_id} groups: {user_groups}")
            
        except Exception as e:
            print(f"[WS] Failed to list user groups: {e}")
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Failed to list user groups: {str(e)}',
                'timestamp': self.get_timestamp()
            }))

    async def group_check(self, event):
        """Handle group check messages"""
        print(f"[WS] Group check received for group {event.get('group_name')}")
        # This confirms the user is a member of this group

    async def _unsubscribe_from_groups(self, groups):
        """Unsubscribe from specific groups"""
       
        
        for raw_group in groups or []:
            group_name = raw_group.replace(':', '_')
            try:
                await self.channel_layer.group_discard(group_name, self.channel_name)
                logger.info("[WS] User %s left group %s", self.user.id, group_name)
            except Exception as e:
                logger.error("[WS] Failed to leave group %s: %s", group_name, e)

        await self.send(text_data=json.dumps({
            'type': 'jobs_unsubscribed',
            'message': f'Unsubscribed from {len(groups)} groups',
            'groups': groups,
            'timestamp': self.get_timestamp()
        }))


    async def disconnect(self, close_code):
        """Clean up when user disconnects"""
        try:
            # Remove user from all Redis groups
            if redis_client and self.group_memberships:
                for group_name in self.group_memberships:
                    try:
                        redis_client.srem(f"group_members:{group_name}", self.user.id)
                        print(f"[WS] Removed user {self.user.id} from Redis group {group_name}")
                    except Exception as e:
                        print(f"[WS] Failed to remove user from Redis group {group_name}: {e}")
            
            # Clear group memberships
            self.group_memberships.clear()
            
        except Exception as e:
            print(f"[WS] Error during disconnect cleanup: {e}")
        
        await super().disconnect(close_code)
