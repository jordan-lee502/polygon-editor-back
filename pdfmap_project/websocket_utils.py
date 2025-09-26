# pdfmap_project/websocket_utils.py
"""
Utility functions for sending WebSocket messages
"""
import json
import logging
import redis
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from django.conf import settings
import time
from pdfmap_project.events.envelope import EventEnvelope, EventType, JobType
from pdfmap_project.events.sequencer import sequence_manager

# Redis connection for tracking group memberships
redis_client = redis.Redis.from_url(getattr(settings, 'REDIS_URL', 'redis://localhost:6379/0'))

def test_redis_connection():
    """Test Redis connection"""
    try:
        redis_client.ping()
        print("Redis connection successful")
        logger.info("Redis connection successful")
        return True
    except Exception as e:
        print(f"Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        return False

logger = logging.getLogger(__name__)

def get_group_members(group_name):
    """Get all user IDs currently in a WebSocket group"""
    try:
        redis_key = f"group_members:{group_name}"
        print(f"Looking for Redis key: {redis_key}")
        
        # Get all user IDs from Redis set
        user_ids = redis_client.smembers(redis_key)
        print(f"Raw Redis result: {user_ids}")
        
        # Convert from bytes to integers
        result = [int(user_id.decode('utf-8')) for user_id in user_ids]
        print(f"Group {group_name} has {len(result)} members: {result}")
        logger.info(f"Group {group_name} has {len(result)} members: {result}")
        return result
    except Exception as e:
        logger.error(f"Failed to get group members for {group_name}: {e}")
        print(f"Failed to get group members for {group_name}: {e}")
        return []

def store_notifications_in_db(notifications):
    """Store notifications in database"""
    from workspace.models import Notification, Workspace
    from django.contrib.auth.models import User
    
    print(f"Storing {len(notifications)} notifications in database")
    logger.info(f"Storing {len(notifications)} notifications in database")
    
    notification_objects = []
    for notif in notifications:
        try:
            print(f"Processing notification  to store data for user {notif['user_id']}, project {notif['project_id']}")
            # Get the user object
            user = User.objects.get(id=notif['user_id'])
            
            # Get workspace object if project_id is provided
            workspace = None
            if notif['project_id']:
                workspace = Workspace.objects.get(id=notif['project_id'])
            
            notification_objects.append(
                Notification(
                    type=notif['type'],
                    payload_json=notif['payload_json'],
                    project=workspace,
                    user=user
                )
            )
            print(f"Created notification object for user {user.id}")
        except (Workspace.DoesNotExist, User.DoesNotExist) as e:
            logger.warning(f"Failed to create notification: {e}")
            print(f"Failed to create notification: {e}")
            continue
    
    if notification_objects:
        try:
            created = Notification.objects.bulk_create(notification_objects)
            print(f"Successfully created {len(created)} notifications in database")
            logger.info(f"Successfully created {len(created)} notifications in database")
        except Exception as e:
            print(f"Error bulk creating notifications: {e}")
            logger.error(f"Error bulk creating notifications: {e}")
    else:
        print("No notification objects to create")
        logger.warning("No notification objects to create")

def send_notification_to_user(user_id, title, message, level='info', data=None):
    """Send notification to a specific user"""
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    notification_data = {
        'type': 'notification_message',
        'title': title,
        'message': message,
        'level': level,
        'data': data or {},
        'timestamp': int(time.time() * 1000)
    }

    group_name = f"notifications_{user_id}"

    async_to_sync(channel_layer.group_send)(
        group_name,
        notification_data
    )

    logger.info(f"Notification sent to user {user_id}: {title}")

def send_job_update_to_user(user_id, job_id, status, progress=0, message='', result=None):
    """Send job update to a specific user"""
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    job_data = {
        'type': 'job_update',
        'job_id': job_id,
        'status': status,
        'progress': progress,
        'message': message,
        'result': result or {},
        'timestamp': int(time.time() * 1000)
    }

    # Send to user-specific job group
    user_group = f"jobs_{user_id}"
    async_to_sync(channel_layer.group_send)(
        user_group,
        job_data
    )

    # Also send to specific job group (for multiple subscribers)
    job_group = f"job_{job_id}"
    async_to_sync(channel_layer.group_send)(
        job_group,
        job_data
    )

    logger.info(f"Job update sent for job {job_id} to user {user_id}: {status}")

def send_polygon_update_to_user(user_id, polygon_id, action, data=None):
    """Send polygon update to a specific user"""
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    polygon_data = {
        'type': 'polygon_update',
        'polygon_id': polygon_id,
        'action': action,  # created, updated, deleted
        'data': data or {},
        'timestamp': int(time.time() * 1000)
    }

    group_name = f"user_{user_id}"

    async_to_sync(channel_layer.group_send)(
        group_name,
        polygon_data
    )

    logger.info(f"Polygon update sent to user {user_id}: {action} polygon {polygon_id}")

def broadcast_notification(title, message, level='info', data=None):
    """Broadcast notification to all connected users"""
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    notification_data = {
        'type': 'notification_message',
        'title': title,
        'message': message,
        'level': level,
        'data': data or {},
        'timestamp': int(time.time() * 1000)
    }

    # Broadcast to all notification groups (this would need to be implemented
    # with a way to track all active notification groups)
    logger.info(f"Broadcast notification: {title}")



def send_notification_to_job_group(job_id, project_id, title, level='info'):
    """Send notification to job group and store in DB"""
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    # Get actual group members from WebSocket groups
    group_name = f"job_{job_id}"
    print(f"[DEBUG] Looking for job group: {group_name}")
    
    # Check what Redis keys exist
    if redis_client:
        try:
            # Get all keys that match the pattern
            pattern = f"group_members:job_*"
            all_keys = redis_client.keys(pattern)
            print(f"[DEBUG] All job group keys in Redis: {all_keys}")
            
            # Check specific key
            specific_key = f"group_members:{group_name}"
            exists = redis_client.exists(specific_key)
            print(f"[DEBUG] Key {specific_key} exists: {exists}")
            
            if exists:
                members = redis_client.smembers(specific_key)
                print(f"[DEBUG] Members in {specific_key}: {members}")
        except Exception as e:
            print(f"[DEBUG] Error checking Redis keys: {e}")
    
    job_user_ids = get_group_members(group_name)
    print(f"[DEBUG] Final result for group {group_name}: {job_user_ids}")

    if not job_user_ids:
        logger.warning(f"No active users found in job group {group_name}")
        return

    seq = sequence_manager.get_next_sequence(str(job_id))
    envelope = EventEnvelope(
        event_type=EventType.NOTIFICATION,
        task_id=str(job_id),
        job_type=JobType.POLYGON_EXTRACTION,
        project_id=str(project_id),
        user_id=0,  # System notification
        seq=seq,
        ts=int(time.time() * 1000),
        detail_url=f"/workspaces/{job_id}/",
        meta={
            'title': title,
            'level': level,
            'job_id': job_id,
        }
    )
    print("asdfasdfadsfads", envelope)
    print("asdddddddddddddddddddddd", job_user_ids)
    # Store notification for each active user in the job group
    notifications = []
    for user_id in job_user_ids:
        notifications.append({
            'type': 'info',  # Use valid NotificationType   
            'payload_json': {
                'title': title,
                'level': level,
                'job_id': job_id,
                'notification_type': 'job_notification'
            },
            'project_id': None,  # Job notifications don't have project_id
            'user_id': user_id,
            'link': f"/workspaces/{job_id}/"
        })

    store_notifications_in_db(notifications)

    # Send envelope to job group
    envelope_data = envelope.to_dict()
    envelope_data["type"] = "event_message"
    
    async_to_sync(channel_layer.group_send)(
        group_name,
        envelope_data
    )

    logger.info(f"Notification sent to job group {job_id} for {len(job_user_ids)} active users: {title}")

def send_notification_to_project_group(project_id, title, level='info'):
    """Send notification to project group and store in DB"""
    print(f"Sending notification to project group {project_id}: {title}")
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    # Get actual group members from WebSocket groups
    group_name = f"project_{project_id}"
    project_user_ids = get_group_members(group_name)
    
    if not project_user_ids:
        logger.warning(f"No active users found in project group {group_name}")
        return
    
    seq = sequence_manager.get_next_sequence(str(project_id))
    envelope = EventEnvelope(
        event_type=EventType.NOTIFICATION,
        task_id=str(project_id),
        job_type=JobType.PDF_EXTRACTION,  # or appropriate job type
        project_id=str(project_id),
        user_id=0,  # System notification
        seq=seq,
        ts=int(time.time() * 1000),
        detail_url=f"/workspaces/{project_id}/",
        meta={
            'title': title,
            'level': level,
            'project_id': project_id,
        }
    )

    # Store notification for each active user in the project group
    notifications = []
    for user_id in project_user_ids:
        notifications.append({
            'type': 'info',  # Use valid NotificationType
            'payload_json': {
                'title': title,
                'level': level,
                'project_id': project_id,
                'notification_type': 'project_notification'
            },
            'project_id': project_id,
            'user_id': user_id,
            'link': f"/workspaces/{project_id}/"
        })

    # Store in database
    store_notifications_in_db(notifications)

    # Send envelope to project group
    envelope_data = envelope.to_dict()
    envelope_data["type"] = "event_message"
    
    async_to_sync(channel_layer.group_send)(
        group_name,
        envelope_data
    )

    logger.info(f"Notification sent to project group {project_id} for {len(project_user_ids)} active users: {title}")
