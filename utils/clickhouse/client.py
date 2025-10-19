import logging
import threading
import time
from typing import Dict, List, Tuple

import clickhouse_connect
from decouple import config

from blockchains.models import Event

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self):
        logger.info("Initializing ClickHouse client")
        self._lock = threading.Lock()
        self._connection_config = {
            "host": config("CLICKHOUSE_HOST"),
            "port": config("CLICKHOUSE_PORT"),
            "user": config("CLICKHOUSE_USER"),
            "password": config("CLICKHOUSE_PASSWORD", default=""),
            "verify": False,
            "connect_timeout": 10,
            "send_receive_timeout": 30,
            "pool_mgr": None,  # Disable connection pooling to avoid session conflicts
        }
        self.network_name = config("NETWORK_NAME")
        self.protocol_name = config("PROTOCOL_NAME")
        self.db_name = f"{self.protocol_name}_{self.network_name}"

        logger.info("ClickHouse client initialized successfully")

    def _get_client(self):
        """Get a fresh client connection for each operation to avoid session locking"""
        return clickhouse_connect.get_client(**self._connection_config)

    def _execute_with_retry(self, operation, max_retries=3, retry_delay=1):
        """Execute an operation with retry logic for session lock errors"""
        last_exception = None

        for attempt in range(max_retries):
            try:
                with self._lock:
                    client = self._get_client()
                    result = operation(client)
                    client.close()
                    return result
            except Exception as e:
                last_exception = e
                error_str = str(e)

                # Check if it's a session lock error
                if "SESSION_IS_LOCKED" in error_str or "373" in error_str:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2**attempt)  # Exponential backoff
                        logger.warning(
                            f"Session locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"Session lock persisted after {max_retries} attempts"
                        )
                        raise
                else:
                    # Non-session-lock error, don't retry
                    raise

        raise last_exception

    def create_database(self):
        logger.info(f"Creating database {self.db_name} if it doesn't exist")

        def operation(client):
            return client.command(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")

        result = self._execute_with_retry(operation)
        logger.info(
            f"Database {self.db_name} created successfully with status: {result}"
        )

    def drop_database(self):
        logger.info(f"Dropping database {self.db_name} if it exists")

        def operation(client):
            return client.command(f"DROP DATABASE IF EXISTS {self.db_name}")

        result = self._execute_with_retry(operation)
        logger.info(
            f"Database {self.db_name} dropped successfully with status: {result}"
        )

    def create_table(self, table_name: str, columns: tuple):
        try:
            logger.info(
                f"Creating table {table_name} in database {self.db_name} if it doesn't exist"
            )
            columns_str = ", ".join(f'"{name}" {type}' for name, type in columns)

            def operation(client):
                return client.command(
                    f"CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name} ({columns_str}) ENGINE = Log"
                )

            result = self._execute_with_retry(operation)
            logger.info(
                f"Table {table_name} created successfully with status: {result}"
            )
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")

    def create_event_table(self, event: Event):
        columns = event._get_clickhouse_columns()
        self.create_table(event.name, columns)

    def insert_rows(self, table_name: str, rows: List[Dict]):
        logger.info(f"Inserting {len(rows)} rows into table {table_name}")
        try:

            def operation(client):
                return client.insert(f"{self.db_name}.{table_name}", rows)

            result = self._execute_with_retry(operation)
            logger.info(f"Rows inserted successfully with status: {result}")
        except Exception as e:
            logger.error(f"Error inserting rows into table {table_name}: {e}")

    def insert_event_logs(self, event: Event, rows: List[Dict]):
        self.insert_rows(event.name, rows)

    def select_rows(self, table_name):
        query = f"SELECT * FROM {self.db_name}.{table_name}"

        def operation(client):
            return client.query(query)

        result = self._execute_with_retry(operation)
        return result.result_rows

    def select_event_rows(self, event: Event):
        return self.select_rows(event.name)

    def execute_query(self, query: str, parameters: Dict = None):
        def operation(client):
            if parameters:
                return client.query(query, parameters=parameters)
            return client.query(query)

        return self._execute_with_retry(operation)

    def truncate_table(self, table_name: str):
        """Truncate a table (remove all rows but keep structure)"""
        try:
            logger.info(f"Truncating table {table_name} in database {self.db_name}")

            def operation(client):
                return client.command(f"TRUNCATE TABLE {self.db_name}.{table_name}")

            result = self._execute_with_retry(operation)
            logger.info(
                f"Table {table_name} truncated successfully with status: {result}"
            )
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")
            raise

    def delete_oracle_records_by_asset_source(
        self, asset_source_pairs: List[Tuple[str, str]]
    ):
        """Delete oracle records for specific asset-source combinations"""
        if not asset_source_pairs:
            logger.warning("No asset-source pairs provided for deletion")
            return

        # All oracle tables that contain asset and asset_source columns
        oracle_tables = {
            # Latest tables (ReplacingMergeTree)
            "PriceLatestEventRawNumerator": "ReplacingMergeTree",
            "PriceLatestEventRawDenominator": "ReplacingMergeTree",
            "PriceLatestEventRawMaxCap": "ReplacingMergeTree",
            "PriceLatestEventRawMultiplier": "ReplacingMergeTree",
            "PriceLatestTransactionRawNumerator": "ReplacingMergeTree",
            "PriceLatestTransactionRawMultiplier": "ReplacingMergeTree",
            "PriceVerificationRecords": "Log",
        }

        deleted_counts = {}

        for table_name, table_type in oracle_tables.items():
            try:
                # Build WHERE clause for asset-source pairs
                conditions = []
                for asset, asset_source in asset_source_pairs:
                    conditions.append(
                        f"(asset = '{asset}' AND asset_source = '{asset_source}')"
                    )

                where_clause = " OR ".join(conditions)

                # Execute DELETE for tables that support it (not Views)
                if table_type in ["Log", "MergeTree", "ReplacingMergeTree"]:
                    delete_query = (
                        f"DELETE FROM {self.db_name}.{table_name} WHERE {where_clause}"
                    )
                    logger.info(f"Deleting from {table_name}: {delete_query}")

                    def delete_operation(client):
                        return client.command(delete_query)

                    self._execute_with_retry(delete_operation)
                    deleted_counts[table_name] = "success"
                    logger.info(
                        f"Deleted records from {table_name} for {len(asset_source_pairs)} asset-source pairs"
                    )
                    time.sleep(
                        0.5
                    )  # Add delay between operations to prevent session conflicts
                else:
                    logger.info(
                        f"Skipping {table_name} (type: {table_type}) - views are computed"
                    )

            except Exception as e:
                logger.warning(f"Could not delete from table {table_name}: {e}")
                deleted_counts[table_name] = f"error: {str(e)}"
                continue

        logger.info(f"Oracle records deletion completed. Results: {deleted_counts}")
        return deleted_counts

    def optimize_table(self, table_name: str):
        if table_name in [
            "CollateralConfigurationChanged",
            "EModeAssetCategoryChanged",
            "EModeCategoryAdded",
            "AssetSourceUpdated",
            "TokenMetadata",
            "AssetSourceTokenMetadata",
            "NetworkBlockInfo",
            "Balances_v2",
        ]:

            def operation(client):
                return client.command(
                    f"OPTIMIZE TABLE {self.db_name}.Latest{table_name} FINAL;"
                )

            self._execute_with_retry(operation)
            logger.info(f"Optimized table {table_name} in database {self.db_name}")

        if table_name in [
            "EventRawNumerator",
            "EventRawDenominator",
            "EventRawMaxCap",
            "EventRawMultiplier",
            "TransactionRawNumerator",
            "TransactionRawMultiplier",
        ]:

            def operation(client):
                return client.command(
                    f"OPTIMIZE TABLE {self.db_name}.PriceLatest{table_name} FINAL;"
                )

            self._execute_with_retry(operation)
            logger.info(f"Optimized table {table_name} in database {self.db_name}")

        if table_name in [
            "CollateralLiquidityIndex",
            "DebtLiquidityIndex",
        ]:

            def operation(client):
                return client.command(
                    f"OPTIMIZE TABLE {self.db_name}.{table_name} FINAL;"
                )

            self._execute_with_retry(operation)
            logger.info(f"Optimized table {table_name} in database {self.db_name}")


clickhouse_client = ClickHouseClient()
