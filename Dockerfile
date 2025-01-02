FROM python:3.10-bullseye

ENV PYTHONDONTWRITEBYTECODE 1

ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install build-essential

WORKDIR /code/

# Install Doppler CLI
RUN apt-get install -y apt-transport-https ca-certificates curl gnupg && \
    curl -sLf --retry 3 --tlsv1.2 --proto "=https" 'https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key' | gpg --dearmor -o /usr/share/keyrings/doppler-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/doppler-archive-keyring.gpg] https://packages.doppler.com/public/cli/deb/debian any-version main" | tee /etc/apt/sources.list.d/doppler-cli.list && \
    apt-get update && \
    apt-get -y install doppler vim screen

COPY requirements.txt /code/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /code/

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

COPY ./bin/start-celery-events /start-celery-events
RUN sed -i 's/\r$//g' /start-celery-events
RUN chmod +x /start-celery-events

COPY ./bin/start-websocket-transactions /start-websocket-transactions
RUN sed -i 's/\r$//g' /start-websocket-transactions
RUN chmod +x /start-websocket-transactions

COPY ./bin/start-sequencer /start-sequencer
RUN sed -i 's/\r$//g' /start-sequencer
RUN chmod +x /start-sequencer

WORKDIR /code/

CMD ["/start-webserver"]
