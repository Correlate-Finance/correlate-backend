import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "correlate.settings")

app = Celery("correlate")
app.config_from_object("django.conf:settings", namespace="CELERY")

app.conf.update(
    task_concurrency=2,  # Use 2 threads for concurrency
    worker_prefetch_multiplier=1,  # Prefetch one task at a time
    worker_heartbeat=60,  # Send a heartbeat every minute
)

app.autodiscover_tasks()
