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
        logger.info("Redis connection successful")
        return True
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return False

logger = logging.getLogger(__name__)

def get_group_members(group_name):
    """Get all user IDs currently in a WebSocket group"""
    try:
        redis_key = f"group_members:{group_name}"
        
        user_ids = redis_client.smembers(redis_key)
        print(f"Raw Redis result: {user_ids}")
        result = [int(user_id.decode('utf-8')) for user_id in user_ids]
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
            user = User.objects.get(id=notif['user_id'])
            
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

    user_group = f"jobs_{user_id}"
    async_to_sync(channel_layer.group_send)(
        user_group,
        job_data
    )

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
        'action': action,
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

    logger.info(f"Broadcast notification: {title}")



def send_notification_to_job_group(job_id, project_id, title, level='info'):
    """Send notification to job group and store in DB"""
    channel_layer = get_channel_layer()
    if not channel_layer:
        logger.error("Channel layer not configured")
        return

    group_name = f"job_{job_id}"
    
    if redis_client:
        try:
            pattern = f"group_members:job_*"
            all_keys = redis_client.keys(pattern)
            
            specific_key = f"group_members:{group_name}"
            exists = redis_client.exists(specific_key)
            
            if exists:
                members = redis_client.smembers(specific_key)
        except Exception as e:
            logger.error(f"Error checking Redis keys: {e}")
    
    job_user_ids = get_group_members(group_name)

    if not job_user_ids:
        logger.warning(f"No active users found in job group {group_name}")
        return

    seq = sequence_manager.get_next_sequence(str(job_id))
    envelope = EventEnvelope(
        event_type=EventType.NOTIFICATION,
        task_id=str(job_id),
        job_type=JobType.POLYGON_EXTRACTION,
        project_id=str(project_id),
        user_id=0,
        seq=seq,
        ts=int(time.time() * 1000),
        detail_url=f"/workspaces/{project_id}/pages/{job_id}/",
        meta={
            'title': title,
            'level': level,
            'job_id': job_id,
        }
    )
    notifications = []
    for user_id in job_user_ids:
        notifications.append({
            'type': 'info',
            'payload_json': {
                'title': title,
                'level': level,
                'job_id': job_id,
                'notification_type': 'job_notification'
            },
            'project_id': None,
            'user_id': user_id,
            'link': f"/workspaces/{job_id}/"
        })

    store_notifications_in_db(notifications)

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

    group_name = f"project_{project_id}"
    project_user_ids = get_group_members(group_name)
    
    if not project_user_ids:
        logger.warning(f"No active users found in project group {group_name}")
        return
    
    seq = sequence_manager.get_next_sequence(str(project_id))
    envelope = EventEnvelope(
        event_type=EventType.NOTIFICATION,
        task_id=str(project_id),
        job_type=JobType.PDF_EXTRACTION,
        project_id=str(project_id),
        user_id=0,
        seq=seq,
        ts=int(time.time() * 1000),
        detail_url=f"/workspaces/{project_id}/",
        meta={
            'title': title,
            'level': level,
            'project_id': project_id,
        }
    )

    notifications = []
    for user_id in project_user_ids:
        notifications.append({
            'type': 'info',
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

    store_notifications_in_db(notifications)

    envelope_data = envelope.to_dict()
    envelope_data["type"] = "event_message"
    
    async_to_sync(channel_layer.group_send)(
        group_name,
        envelope_data
    )

    logger.info(f"Notification sent to project group {project_id} for {len(project_user_ids)} active users: {title}")
