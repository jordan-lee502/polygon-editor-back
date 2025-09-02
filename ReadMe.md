# 4) (Optional) Start Celery Beat for periodic tasks
python -m celery -A pdfmap_project worker -l info -P solo -Q sync,celery,process

# 5) Enqueue a single workspace via Django management command
python manage.py enqueue_tto_sync --workspace-id 123 --verbose

# 6) Sync all workspaces via Django management command
python manage.py sync_all_workspaces_tto --all

# 7) 
python manage.py process_pending_workspaces