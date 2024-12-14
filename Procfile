webserver: doppler run --command "python manage.py runserver"
redis: redis-server
celery-default: doppler run --command "celery -A liquidations_v2 worker --concurrency 2 -Ofair --loglevel info -E -n default"
celery-events: doppler run --command "celery -A liquidations_v2 worker --concurrency 8 -Ofair --loglevel info -E -n events"