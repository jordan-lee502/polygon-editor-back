# project/urls.py (root)
from django.urls import path
from .views import upload_public_files, upload_page_image

urlpatterns = [
  path("", upload_public_files, name="upload_public_files"),
  path("page-image/", upload_page_image, name="upload_page_image"),
]



