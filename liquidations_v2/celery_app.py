import os

from celery import Celery
from decouple import config

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "liquidations_v2.settings")
app = Celery("app")

app.config_from_object("django.conf:settings")
app.conf.update(BROKER_URL=config('CELERY_BROKER_URL'))

# Load task modules from all registered Django app configs.
app.conf.task_default_queue = "default"
app.autodiscover_tasks()
