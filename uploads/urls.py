# project/urls.py (root)
from django.conf import settings
from django.conf.urls.static import static
from django.urls import path, include
from .views import upload_public_files

urlpatterns = [
  path("", upload_public_files, name="upload_public_files"),
]
