import logging
from typing import Dict, List

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
            password=config("CLICKHOUSE_PASSWORD", default=""),
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

    def create_table(self, table_name: str, columns: tuple):
        try:
            logger.info(
                f"Creating table {table_name} in database {self.db_name} if it doesn't exist"
            )
            columns_str = ", ".join(f'"{name}" {type}' for name, type in columns)
            result = self.client.command(
                f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({columns_str}) ENGINE = Log"
            )
            logger.info(
                f"Table {table_name} created successfully with status: {result}"
            )
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")

    def create_event_table(self, event: Event):
        columns = event._get_clickhouse_columns()
        self.create_table(event.name, columns)

    def drop_event_table(self, event: Event):
        logger.info(
            f"Dropping table {event.name} from database {self.db_name} if it exists"
        )
        result = self.client.command(
            f"DROP TABLE IF EXISTS {self.db_name}.{event.name}"
        )
        logger.info(f"Table {event.name} dropped successfully with status: {result}")

    def insert_rows(self, table_name: str, rows: List[Dict]):
        logger.info(f"Inserting {len(rows)} rows into table {table_name}")
        try:
            result = self.client.insert(f"{self.db_name}.{table_name}", rows)
            logger.info(f"Rows inserted successfully with status: {result}")
        except Exception as e:
            logger.error(f"Error inserting rows into table {table_name}: {e}")

    def insert_event_logs(self, event: Event, rows: List[Dict]):
        self.insert_rows(event.name, rows)

    def select_rows(self, table_name):
        query = f"SELECT * FROM {self.db_name}.{table_name}"
        result = self.client.query(query)
        return result.result_rows

    def select_event_rows(self, event: Event):
        return self.select_rows(event.name)

    def execute_query(self, query: str, parameters: Dict = None):
        if parameters:
            return self.client.query(query, parameters=parameters)
        return self.client.query(query)

    def optimize_table(self, table_name: str):
        if table_name in [
            "CollateralConfigurationChanged",
            "EModeAssetCategoryChanged",
            "EModeCategoryAdded",
            "TokenMetadata",
            "AssetSourceUpdated",
            "AssetSourceTokenMetadata",
            "RawPriceEvent",
        ]:
            self.execute_query(
                f"OPTIMIZE TABLE {self.db_name}.Latest{table_name} FINAL;"
            )
            logger.info(f"Optimized table {table_name} in database {self.db_name}")


clickhouse_client = ClickHouseClient()
