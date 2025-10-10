FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONFAULTHANDLER=1
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_SYSTEM_PYTHON=1
ENV UV_NO_CACHE=1

RUN apt-get update \
    && apt-get update \
    # dependencies for building Python packages
    && apt-get install -y build-essential \
    # psycopg2 dependencies with newer libpq
    && apt-get install -y libpq5 libpq-dev \
    # Translations dependencies
    && apt-get install -y gettext curl \
    # Git
    && apt-get install -y git apt-transport-https ca-certificates gnupg \
    # Chrome and its dependencies
    && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv venv --python /usr/local/bin/python
RUN uv sync --frozen --no-install-project --no-dev

ENV PATH="/app/.venv/bin:$PATH"

COPY ./bin/start-webserver /start-webserver
RUN sed -i 's/\r$//g' /start-webserver
RUN chmod +x /start-webserver

COPY ./bin/start-webserver-local /start-webserver-local
RUN sed -i 's/\r$//g' /start-webserver-local
RUN chmod +x /start-webserver-local

COPY ./bin/start-celery-beat /start-celery-beat
RUN sed -i 's/\r$//g' /start-celery-beat
RUN chmod +x /start-celery-beat

COPY ./bin/start-celery-default /start-celery-default
RUN sed -i 's/\r$//g' /start-celery-default
RUN chmod +x /start-celery-default

COPY ./bin/start-websocket-transactions /start-websocket-transactions
RUN sed -i 's/\r$//g' /start-websocket-transactions
RUN chmod +x /start-websocket-transactions

COPY ./bin/start-websocket-blocks /start-websocket-blocks
RUN sed -i 's/\r$//g' /start-websocket-blocks
RUN chmod +x /start-websocket-blocks

COPY ./bin/start-celery-high /start-celery-high
RUN sed -i 's/\r$//g' /start-celery-high
RUN chmod +x /start-celery-high

# install doppler using official method
RUN curl -sLf --retry 3 --tlsv1.2 --proto "=https" 'https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key' | gpg --dearmor -o /usr/share/keyrings/doppler-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/doppler-archive-keyring.gpg] https://packages.doppler.com/public/cli/deb/debian any-version main" | tee /etc/apt/sources.list.d/doppler-cli.list \
    && apt-get update && apt-get install -y doppler

EXPOSE 8000

COPY . /app

CMD ["doppler", "run", "--", "/start-webserver"]
