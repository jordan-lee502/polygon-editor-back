# pdfmap_project/celery.py
import os
from celery import Celery

# Point Celery at your Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pdfmap_project.settings")

# Create the Celery app
app = Celery("pdfmap_project")

# Load config from Django settings using the "CELERY_" namespace
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks across installed apps (e.g., sync.tasks)
app.autodiscover_tasks()
