from decouple import config

CELERY_BROKER_URL = config("CELERY_BROKER_URL")

CELERYD_TASK_TIME_LIMIT = 60 * 60 * 6  # 6 hours

CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

CELERY_TIMEZONE = "UTC"

CELERY_IGNORE_RESULT = True

# Memory management settings to prevent memory buildup
CELERY_WORKER_MAX_MEMORY_PER_CHILD = 500_000  # 500 MB - worker restarts when exceeded
CELERY_WORKER_MAX_TASKS_PER_CHILD = (
    100  # Increased from 10 - worker restarts after N tasks
)

# Additional memory management settings
CELERY_WORKER_MAX_MEMORY_PER_CHILD_RESTART = (
    True  # Force restart when memory limit reached
)
CELERY_WORKER_POOL_RESTARTS = True  # Enable pool restarts

# Task execution settings
CELERY_ROUTES = {
    "oracles.tasks.InsertTransactionNumeratorTask": {"queue": "High"},
}

CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

CELERY_WORKER_PREFETCH_MULTIPLIER = 1

CELERY_TASK_ALWAYS_EAGER = False

# Worker lifecycle management
CELERY_WORKER_DISABLE_RATE_LIMITS = (
    True  # Disable rate limits for better memory management
)
CELERY_WORKER_SEND_TASK_EVENTS = False  # Reduce memory overhead from task events
