export UV_KEYRING_PROVIDER := "subprocess"

# Show available commands
@_default:
  just --list

# Run all linters to check code quality
@lint:
  echo ">>> Running isort"
  uv run isort --check-only .
  echo ">>> Running ruff linter"
  ruff check .
  echo ">>> Running ruff formatter check"
  ruff format --check .

# Format code using isort and ruff
@format:
  echo ">>> Running isort"
  uv run isort .
  echo ">>> Running ruff linter with autofix"
  ruff check --fix .
  echo ">>> Running ruff formatter"
  ruff format .

# Lock dependencies with uv
@lock:
  uv lock

# Install dependencies from lockfile
@install:
  uv sync --frozen --all-extras

# Set up development environment with required tools
@sysinstall:
  brew install uv pre-commit ruff
  uv python install 3.10
  uv venv --python 3.10
  just install

# Add a new package dependency
@add-dep package_name:
  uv add {{package_name}}
  just lock

# Add a new development dependency
@add-dev-dep package_name:
  uv add --dev {{package_name}}
  just lock

# Update all dependencies to latest versions
@update-deps:
  uv lock --upgrade
  just install

# Launch storage services
@launchstorage:
  docker compose up postgres redis -d

# Run web server
@makemigrations:
  source .venv/bin/activate
  source exportenv.sh
  python manage.py makemigrations

# Run web server
@startweb:
  source .venv/bin/activate
  source exportenv.sh
  python manage.py collectstatic --noinput
  python manage.py migrate
  python manage.py runserver

# Run celery worker
@startcelery:
  source .venv/bin/activate
  source exportenv.sh
  doppler run --command "celery -A liquidations_v2 worker --loglevel info -E -n default_%h -Q default"

# Run celery worker
@startceleryhigh:
  source .venv/bin/activate
  source exportenv.sh
  doppler run --command "celery -A liquidations_v2 worker --loglevel info -E -n High_%h -Q High"

# Run celery beat
@startbeat:
  source .venv/bin/activate
  source exportenv.sh
  doppler run --command "celery -A liquidations_v2 beat --loglevel=info  --scheduler django_celery_beat.schedulers:DatabaseScheduler"

# Run websocket server
@startwsstxns:
  source .venv/bin/activate
  source exportenv.sh
  doppler run --command "python manage.py listen_pending_transactions"

# Run websocket server
@startwssblocks:
  source .venv/bin/activate
  source exportenv.sh
  doppler run --command "python manage.py listen_blocks"

# Launch Django shell
@shell:
  source .venv/bin/activate
  source exportenv.sh
  python manage.py shell

@test:
  source .venv/bin/activate
  source exportenv.sh
  doppler run --command "pytest -v -cov --disable-warnings"
