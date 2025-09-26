# pdfmap_project/urls.py

from django.contrib import admin
from django.urls import path, re_path, include
from django.conf import settings
from django.conf.urls.static import static
from .views import index, health_redis

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include("workspace.urls")),
    path("api/auth/", include("authx.urls")),
    path("health/redis", health_redis, name="health_redis"),
    path("", index),
    re_path(
        r"^(?!static/|media/|api/|admin/).*", index
    ),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
