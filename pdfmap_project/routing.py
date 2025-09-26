# pdfmap_project/routing.py
from django.urls import re_path
from channels.routing import URLRouter

def get_websocket_urlpatterns():
    """Lazy load websocket patterns to avoid AppRegistryNotReady error"""
    from .consumers import JobConsumer, EventConsumer, UserConsumer
    return [
        # Job updates
        re_path(r'ws/jobs/$', JobConsumer.as_asgi()),

        # Event updates (lightweight envelopes)
        re_path(r'ws/events/$', EventConsumer.as_asgi()),

        # User updates
        re_path(r'ws/users/$', UserConsumer.as_asgi()),
    ]

websocket_urlpatterns = get_websocket_urlpatterns()
