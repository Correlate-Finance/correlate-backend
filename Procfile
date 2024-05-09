release: cd correlate && python manage.py migrate

web: cd correlate && ddtrace-run python manage.py runserver 0.0.0.0:$PORT
worker: cd correlate && celery -A correlate worker
