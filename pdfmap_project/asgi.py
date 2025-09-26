"""
ASGI config for pdfmap_project project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/howto/deployment/asgi/
"""

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdfmap_project.settings')

django_asgi_app = get_asgi_application()



def get_websocket_router():
    """Lazy load websocket router to avoid AppRegistryNotReady error"""
    from .routing import websocket_urlpatterns
    return URLRouter(websocket_urlpatterns)

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": AuthMiddlewareStack(
        get_websocket_router()
    ),
})
