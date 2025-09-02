# utils/urls.py

from urllib.parse import urljoin
from django.conf import settings
from django.core.files.storage import default_storage

def to_absolute_media_url(value) -> str:
    """
    Accepts a FileField, FieldFile, or a string path like 'fullpages/...png'
    Returns an absolute URL if possible, else a relative MEDIA_URL-based URL.
    """
    if not value:
        return ""

    # If it's a FileField (e.g., pg.image)
    if hasattr(value, "url"):
        url = value.url  # storage-generated
    else:
        # It's likely a string path, let storage resolve to URL
        url = default_storage.url(str(value).replace("\\", "/"))

    # If storage already produced an absolute URL (S3/CDN), return it
    if url.startswith("http://") or url.startswith("https://"):
        return url

    # Otherwise, prefix with BASE_URL to make it absolute
    base = getattr(settings, "BASE_URL", "").rstrip("/")
    return urljoin(base + "/", url.lstrip("/"))
