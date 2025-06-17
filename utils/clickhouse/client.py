import logging

import clickhouse_connect
from decouple import config

from blockchains.models import Event

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self):
        logger.info("Initializing ClickHouse client")
        self.client = clickhouse_connect.get_client(
            host=config("CLICKHOUSE_HOST"),
            port=config("CLICKHOUSE_PORT"),
            user=config("CLICKHOUSE_USER"),
            password=config("CLICKHOUSE_PASSWORD"),
        )
        self.network_name = config("NETWORK_NAME")
        self.protocol_name = config("PROTOCOL_NAME")
        self.db_name = f"{self.protocol_name}_{self.network_name}"
        logger.info("ClickHouse client initialized successfully")

    def create_database(self):
        logger.info(f"Creating database {self.db_name} if it doesn't exist")
        result = self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        logger.info(
            f"Database {self.db_name} created successfully with status: {result}"
        )

    def drop_database(self):
        logger.info(f"Dropping database {self.db_name} if it exists")
        result = self.client.command(f"DROP DATABASE IF EXISTS {self.db_name}")
        logger.info(
            f"Database {self.db_name} dropped successfully with status: {result}"
        )

    def create_event_table(self, event: Event):
        columns = event._get_clickhouse_columns()
        logger.info(
            f"Creating table {event.name} in database {self.db_name} if it doesn't exist"
        )
        columns_str = ", ".join(f'"{name}" {type}' for name, type in columns)
        try:
            result = self.client.command(
                f"CREATE TABLE IF NOT EXISTS {self.db_name}.{event.name} ({columns_str}) ENGINE = Log"
            )
            logger.info(
                f"Table {event.name} created successfully with status: {result}"
            )
        except Exception as e:
            logger.error(f"Error creating table {event.name}: {e}")

    def drop_event_table(self, event: Event):
        logger.info(
            f"Dropping table {event.name} from database {self.db_name} if it exists"
        )
        result = self.client.command(
            f"DROP TABLE IF EXISTS {self.db_name}.{event.name}"
        )
        logger.info(f"Table {event.name} dropped successfully with status: {result}")


clickhouse_client = ClickHouseClient()
