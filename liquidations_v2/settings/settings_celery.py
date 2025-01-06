from decouple import config

CELERY_BROKER_URL = config("CELERY_BROKER_URL")

CELERYD_TASK_TIME_LIMIT = 60 * 60 * 3  # 3 hours

CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

CELERY_TIMEZONE = "UTC"

CELERY_IGNORE_RESULT = True

CELERY_WORKER_MAX_MEMORY_PER_CHILD = 500_000  # 500 MB

CELERY_WORKER_MAX_TASKS_PER_CHILD = 10

CELERY_ROUTES = {
    # "blockchains.tasks.events.ResetLogsForAppTask": {"queue": "events"},
}

CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

CELERY_WORKER_PREFETCH_MULTIPLIER = 1
