# processing/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from .pdf_processor import process_pending_workspaces
from sync.jobs_sync import process_pending_sync_workspaces  # <- import the new job
from django.conf import settings

def start():
    scheduler = BackgroundScheduler()
    # existing job
    scheduler.add_job(process_pending_workspaces, "interval", seconds=10)
    # new sync job (tune cadence as you like)
    if not settings.DEBUG:
        scheduler.add_job(process_pending_sync_workspaces, "interval", seconds=30)

    scheduler.start()
