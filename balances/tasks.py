import csv
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, List

from celery import Task

from balances.models import BalanceEvent
from liquidations_v2.celery_app import app
from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_NAME
from utils.interfaces.tokens import AaveToken
from utils.rpc import rpc_adapter
from utils.tasks import EventSynchronizeMixin, ParentSynchronizeTaskMixin

logger = logging.getLogger(__name__)


class ChildBalancesSynchronizeTask(EventSynchronizeMixin, Task):
    event_model = BalanceEvent
    clickhouse_client = clickhouse_client
    rpc_adapter = rpc_adapter
    network_name = NETWORK_NAME

    def group_event_logs_by_address(
        self, event_logs: List[Any]
    ) -> Dict[str, List[Any]]:
        grouped_event_logs = {}
        for event_log in event_logs:
            address = event_log.address
            if address not in grouped_event_logs:
                grouped_event_logs[address] = []
            grouped_event_logs[address].append(event_log)
        return grouped_event_logs

    def handle_event_logs(self, network_events: List[Any], event_dicts: Dict):
        for topic_0, event_logs in event_dicts.items():
            grouped_event_logs = self.group_event_logs_by_address(event_logs)
            parsed_event_logs = []
            updated_network_events = []

            filtered_network_events = network_events.filter(topic_0=topic_0)
            for network_event in filtered_network_events:
                event_logs = grouped_event_logs.get(
                    network_event.contract_addresses[0], []
                )
                log_fields = [
                    col_name
                    for col_name, _ in network_event._get_clickhouse_log_columns()
                ]
                all_fields = [
                    col_name for col_name, _ in network_event._get_clickhouse_columns()
                ]
                arg_fields = [field for field in all_fields if field not in log_fields]

                if not event_logs:
                    continue

                timestamps = self.get_timestamps_for_events(event_logs)

                for event_log in event_logs:
                    event_log_args = getattr(event_log, "args")
                    event_values = [
                        getattr(event_log_args, field) for field in arg_fields
                    ]
                    log_values = [
                        getattr(event_log, field)
                        for field in log_fields
                        if field != "blockTimestamp"
                    ]
                    log_values.append(timestamps[event_log.blockNumber])
                    log_values.append(network_event.type)
                    log_values.append(network_event.asset)
                    parsed_event_log = event_values + log_values
                    parsed_event_logs.append(parsed_event_log)

                network_event.logs_count += len(event_logs)
                updated_network_events.append(network_event)

            for i in range(3):
                try:
                    self.clickhouse_client.insert_rows(
                        network_event.name, parsed_event_logs
                    )
                    break
                except Exception as e:
                    logger.error(f"Error inserting rows: {e}")
                    time.sleep(5)

            for i in range(3):
                try:
                    self.clickhouse_client.optimize_table("Balances_v2")
                    break
                except Exception as e:
                    logger.error(f"Error optimizing table: {e}")
                    time.sleep(5)

                logger.info(f"Number of records inserted: {len(parsed_event_logs)}")

            BalanceEvent.objects.bulk_update(updated_network_events, ["logs_count"])

    def post_handle_hook(
        self, network_events: List[Any], start_block: int, end_block: int
    ):
        """
        Post hook to update LatestBalances_v2 with scaled balances from on-chain data.
        Retrieves unique user/asset pairs from synchronized events and updates their
        scaled balances by querying aToken and variableDebtToken contracts.
        """
        try:
            # Get all unique user/asset pairs from the synced events in batches
            all_user_asset_pairs = []
            batch_size = 500
            offset = 0

            while True:
                user_asset_pairs = self._get_unique_user_asset_pairs(
                    start_block, end_block, limit=batch_size, offset=offset
                )
                if not user_asset_pairs:
                    break

                all_user_asset_pairs.extend(user_asset_pairs)
                offset += batch_size

                # If we got fewer results than batch_size, we're done
                if len(user_asset_pairs) < batch_size:
                    break

            if not all_user_asset_pairs:
                logger.info("No user/asset pairs found to update")
                return

            logger.info(
                f"Updating scaled balances for {len(all_user_asset_pairs)} user/asset pairs"
            )

            # Get token address mappings from ClickHouse
            asset_token_mapping = self._get_asset_token_mapping(
                list({asset for _, asset in all_user_asset_pairs})
            )

            # Group users by asset for efficient batch processing
            users_by_asset = defaultdict(list)
            for user, asset in all_user_asset_pairs:
                users_by_asset[asset].append(user)

            # Fetch scaled balances and prepare update data
            updates = []
            for asset, users in users_by_asset.items():
                if asset not in asset_token_mapping:
                    logger.warning(f"No token mapping found for asset {asset}")
                    continue

                atoken_address = asset_token_mapping[asset]["aToken"]
                variable_debt_token_address = asset_token_mapping[asset][
                    "variableDebtToken"
                ]

                # Get collateral scaled balances and indexes in batches of 100
                collateral_balances = {}
                collateral_indexes = {}
                if atoken_address:
                    atoken = AaveToken(atoken_address)
                    for i in range(0, len(users), 100):
                        batch_users = users[i : i + 100]
                        batch_balances = atoken.get_scaled_balance(batch_users)
                        batch_indexes = atoken.get_previous_index(batch_users)
                        collateral_balances.update(batch_balances)
                        collateral_indexes.update(batch_indexes)

                # Get variable debt scaled balances and indexes in batches of 100
                debt_balances = {}
                debt_indexes = {}
                if variable_debt_token_address:
                    debt_token = AaveToken(variable_debt_token_address)
                    for i in range(0, len(users), 100):
                        batch_users = users[i : i + 100]
                        batch_balances = debt_token.get_scaled_balance(batch_users)
                        batch_indexes = debt_token.get_previous_index(batch_users)
                        debt_balances.update(batch_balances)
                        debt_indexes.update(batch_indexes)

                # Prepare rows for ClickHouse insert (user, asset, collateral, debt, collateral_index, debt_index)
                for user in users:
                    collateral = collateral_balances.get(user, 0)
                    debt = debt_balances.get(user, 0)
                    collateral_idx = collateral_indexes.get(user, 0)
                    debt_idx = debt_indexes.get(user, 0)
                    # Note: updated_at will be set by DEFAULT now64() in ClickHouse
                    updates.append(
                        (user, asset, collateral, debt, collateral_idx, debt_idx)
                    )

            # Batch insert into LatestBalances_v2
            if updates:
                for i in range(3):
                    try:
                        # Use direct client access to specify column names
                        def insert_operation(client):
                            return client.insert(
                                f"{self.clickhouse_client.db_name}.LatestBalances_v2",
                                updates,
                                column_names=[
                                    "user",
                                    "asset",
                                    "collateral_scaled_balance",
                                    "variable_debt_scaled_balance",
                                    "collateral_index",
                                    "debt_index",
                                ],
                            )

                        self.clickhouse_client._execute_with_retry(insert_operation)
                        logger.info(
                            f"Successfully updated {len(updates)} scaled balances with indexes in LatestBalances_v2"
                        )
                        break
                    except Exception as e:
                        logger.error(
                            f"Error inserting scaled balances (attempt {i + 1}/3): {e}"
                        )
                        if i < 2:
                            time.sleep(5)

                # Optimize table after updates
                for i in range(3):
                    try:
                        self.clickhouse_client.optimize_table("LatestBalances_v2")
                        break
                    except Exception as e:
                        logger.error(f"Error optimizing LatestBalances_v2: {e}")
                        time.sleep(5)

        except Exception as e:
            logger.error(f"Error in post_handle_hook: {e}", exc_info=True)

    def _get_unique_user_asset_pairs(
        self, start_block: int, end_block: int, limit: int = 500, offset: int = 0
    ):
        """
        Query ClickHouse to get unique user/asset pairs from recently synced events
        within the specified block range, with pagination support.
        """
        query = f"""
        SELECT DISTINCT user, asset
        FROM (
            SELECT `from` AS user, asset FROM aave_ethereum.Burn
            WHERE blockNumber BETWEEN {start_block} AND {end_block}
            UNION ALL
            SELECT onBehalfOf AS user, asset FROM aave_ethereum.Mint
            WHERE blockNumber BETWEEN {start_block} AND {end_block}
            UNION ALL
            SELECT _from AS user, asset FROM aave_ethereum.BalanceTransfer
            WHERE blockNumber BETWEEN {start_block} AND {end_block}
            UNION ALL
            SELECT _to AS user, asset FROM aave_ethereum.BalanceTransfer
            WHERE blockNumber BETWEEN {start_block} AND {end_block}
        )
        WHERE user != '0x0000000000000000000000000000000000000000'
        ORDER BY user, asset
        LIMIT {limit} OFFSET {offset}
        """
        result = self.clickhouse_client.execute_query(query)
        return [(row[0], row[1]) for row in result.result_rows]

    def _get_asset_token_mapping(self, assets: List[str]):
        """
        Get aToken and variableDebtToken addresses for given assets from ClickHouse.
        """
        if not assets:
            return {}

        assets_str = ",".join([f"'{asset}'" for asset in assets])
        query = f"""
        SELECT asset, aToken, variableDebtToken
        FROM aave_ethereum.view_LatestAssetConfiguration
        WHERE asset IN ({assets_str})
        """
        result = self.clickhouse_client.execute_query(query)
        return {
            row[0]: {"aToken": row[1], "variableDebtToken": row[2]}
            for row in result.result_rows
        }


ChildBalancesSynchronizeTask = app.register_task(ChildBalancesSynchronizeTask())


class ParentBalancesSynchronizeTask(ParentSynchronizeTaskMixin, Task):
    event_model = BalanceEvent
    child_task = ChildBalancesSynchronizeTask


ParentBalancesSynchronizeTask = app.register_task(ParentBalancesSynchronizeTask())


class BalancesBackfillTask(Task):
    """
    Standalone task that retrieves all user/asset pairs from ClickHouse,
    stores them in a CSV file, then iteratively fetches scaled balances
    and indexes to update LatestBalances_v2.
    """

    clickhouse_client = clickhouse_client

    def run(self, csv_output_path: str = "/tmp"):
        """
        Main task execution:
        1. Query all user/asset pairs from ClickHouse
        2. Write them to CSV
        3. Read CSV and fetch scaled balances + indexes
        4. Update LatestBalances_v2
        """
        logger.info("Starting balances backfill task")

        # Step 1: Export user/asset pairs to CSV
        csv_filepath = self._export_user_asset_pairs_to_csv(csv_output_path)
        if not csv_filepath:
            logger.info("No user/asset pairs found to backfill")
            return

        # Step 2: Read CSV and fetch scaled balances + indexes
        logger.info(f"Reading user/asset pairs from {csv_filepath}")
        self._backfill_from_csv(csv_filepath)

        logger.info("Balances backfill task completed")

    def _export_user_asset_pairs_to_csv(self, output_path: str):
        """
        Query ClickHouse for all unique user/asset pairs and write to CSV.
        Streams results directly to CSV to minimize memory usage.
        Processes each event table separately to avoid timeout issues.
        Returns the CSV file path, or None if no pairs found.
        """
        try:
            logger.info("Querying all user/asset pairs from ClickHouse")

            # Write to CSV file while streaming results
            csv_filename = f"user_asset_pairs_{int(time.time())}.csv"
            csv_filepath = os.path.join(output_path, csv_filename)

            # Try LatestBalances_v2 first (most efficient)
            total_pairs = self._export_from_latest_balances(csv_filepath)

            if total_pairs > 0:
                logger.info(
                    f"Exported {total_pairs} user/asset pairs from LatestBalances_v2 to {csv_filepath}"
                )
                return csv_filepath

            # Fallback: Query from event tables separately
            logger.info("LatestBalances_v2 is empty, querying from event tables...")
            total_pairs = self._export_from_event_tables(csv_filepath)

            if total_pairs == 0:
                logger.info("No user/asset pairs found")
                if os.path.exists(csv_filepath):
                    os.remove(csv_filepath)  # Clean up empty file
                return None

            logger.info(f"Exported {total_pairs} user/asset pairs to {csv_filepath}")
            return csv_filepath

        except Exception as e:
            logger.error(f"Error exporting user/asset pairs to CSV: {e}", exc_info=True)
            return None

    def _export_from_latest_balances(self, csv_filepath: str) -> int:
        """
        Export user/asset pairs from LatestBalances_v2 table.
        Returns the count of pairs exported.
        """
        try:
            total_pairs = 0
            batch_size = 1000
            offset = 0

            with open(csv_filepath, "w", newline="") as csvfile:
                fieldnames = ["user", "asset"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                while True:
                    query = f"""
                    SELECT user, asset
                    FROM aave_ethereum.LatestBalances_v2
                    WHERE user != '0x0000000000000000000000000000000000000000'
                    GROUP BY user, asset
                    ORDER BY user, asset
                    LIMIT {batch_size} OFFSET {offset}
                    """

                    result = self.clickhouse_client.execute_query(query)
                    if not result.result_rows:
                        break

                    for row in result.result_rows:
                        writer.writerow({"user": row[0], "asset": row[1]})

                    total_pairs += len(result.result_rows)
                    offset += batch_size
                    logger.info(
                        f"Retrieved {total_pairs} user/asset pairs from LatestBalances_v2..."
                    )

                    if len(result.result_rows) < batch_size:
                        break

            return total_pairs
        except Exception as e:
            logger.warning(f"Could not query LatestBalances_v2: {e}")
            return 0

    def _export_from_event_tables(self, csv_filepath: str) -> int:
        """
        Export user/asset pairs by querying each event table separately.
        Uses a set to deduplicate pairs across tables.
        Returns the count of unique pairs exported.
        """
        seen_pairs = set()

        # Define table queries
        table_queries = [
            (
                "Burn",
                "SELECT DISTINCT `from` AS user, asset FROM aave_ethereum.Burn WHERE `from` != '0x0000000000000000000000000000000000000000'",
            ),
            (
                "Mint",
                "SELECT DISTINCT onBehalfOf AS user, asset FROM aave_ethereum.Mint WHERE onBehalfOf != '0x0000000000000000000000000000000000000000'",
            ),
            (
                "BalanceTransfer_from",
                "SELECT DISTINCT _from AS user, asset FROM aave_ethereum.BalanceTransfer WHERE _from != '0x0000000000000000000000000000000000000000'",
            ),
            (
                "BalanceTransfer_to",
                "SELECT DISTINCT _to AS user, asset FROM aave_ethereum.BalanceTransfer WHERE _to != '0x0000000000000000000000000000000000000000'",
            ),
        ]

        with open(csv_filepath, "w", newline="") as csvfile:
            fieldnames = ["user", "asset"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for table_name, query in table_queries:
                logger.info(f"Querying user/asset pairs from {table_name}...")

                try:
                    result = self.clickhouse_client.execute_query(query)
                    new_pairs = 0

                    for row in result.result_rows:
                        pair = (row[0], row[1])
                        if pair not in seen_pairs:
                            seen_pairs.add(pair)
                            writer.writerow({"user": pair[0], "asset": pair[1]})
                            new_pairs += 1

                    logger.info(
                        f"Found {new_pairs} new unique pairs from {table_name} (total: {len(seen_pairs)})"
                    )

                except Exception as e:
                    logger.error(f"Error querying {table_name}: {e}")
                    continue

        return len(seen_pairs)

    def _backfill_from_csv(self, csv_filepath: str):
        """
        Read user/asset pairs from CSV and fetch scaled balances + indexes
        from on-chain, then update LatestBalances_v2.
        """
        try:
            # Read CSV file
            user_asset_pairs = []
            with open(csv_filepath, "r") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    user_asset_pairs.append((row["user"], row["asset"]))

            logger.info(f"Loaded {len(user_asset_pairs)} user/asset pairs from CSV")

            # Get unique assets to fetch token mappings
            unique_assets = list({asset for _, asset in user_asset_pairs})
            asset_token_mapping = self._get_asset_token_mapping(unique_assets)

            # Group users by asset for efficient batch processing
            users_by_asset = defaultdict(list)
            for user, asset in user_asset_pairs:
                users_by_asset[asset].append(user)

            # Process each asset's users
            total_updates = 0
            for asset, users in users_by_asset.items():
                logger.info(f"Processing {len(users)} users for asset {asset}")

                if asset not in asset_token_mapping:
                    logger.warning(f"No token mapping found for asset {asset}")
                    continue

                atoken_address = asset_token_mapping[asset]["aToken"]
                variable_debt_token_address = asset_token_mapping[asset][
                    "variableDebtToken"
                ]

                # Fetch scaled balances and indexes in batches
                updates = []
                batch_size = 100

                for i in range(0, len(users), batch_size):
                    batch_users = users[i : i + batch_size]

                    # Get collateral data
                    collateral_balances = {}
                    collateral_indexes = {}
                    if atoken_address:
                        atoken = AaveToken(atoken_address)
                        collateral_balances = atoken.get_scaled_balance(batch_users)
                        collateral_indexes = atoken.get_previous_index(batch_users)

                    # Get debt data
                    debt_balances = {}
                    debt_indexes = {}
                    if variable_debt_token_address:
                        debt_token = AaveToken(variable_debt_token_address)
                        debt_balances = debt_token.get_scaled_balance(batch_users)
                        debt_indexes = debt_token.get_previous_index(batch_users)

                    # Prepare update rows
                    for user in batch_users:
                        collateral = collateral_balances.get(user, 0)
                        debt = debt_balances.get(user, 0)
                        collateral_idx = collateral_indexes.get(user, 0)
                        debt_idx = debt_indexes.get(user, 0)
                        updates.append(
                            (user, asset, collateral, debt, collateral_idx, debt_idx)
                        )

                    logger.info(
                        f"Fetched balances for batch {i // batch_size + 1} ({len(batch_users)} users)"
                    )

                # Insert into ClickHouse
                if updates:
                    for attempt in range(3):
                        try:

                            def insert_operation(client):
                                return client.insert(
                                    f"{self.clickhouse_client.db_name}.LatestBalances_v2",
                                    updates,
                                    column_names=[
                                        "user",
                                        "asset",
                                        "collateral_scaled_balance",
                                        "variable_debt_scaled_balance",
                                        "collateral_index",
                                        "debt_index",
                                    ],
                                )

                            self.clickhouse_client._execute_with_retry(insert_operation)
                            total_updates += len(updates)
                            logger.info(
                                f"Successfully inserted {len(updates)} records for asset {asset}"
                            )
                            break
                        except Exception as e:
                            logger.error(
                                f"Error inserting records (attempt {attempt + 1}/3): {e}"
                            )
                            if attempt < 2:
                                time.sleep(5)

            # Optimize table after all updates
            logger.info("Optimizing LatestBalances_v2 table")
            for attempt in range(3):
                try:
                    self.clickhouse_client.optimize_table("LatestBalances_v2")
                    break
                except Exception as e:
                    logger.error(f"Error optimizing table: {e}")
                    if attempt < 2:
                        time.sleep(5)

            logger.info(f"Backfill completed: {total_updates} total records updated")

        except Exception as e:
            logger.error(f"Error during backfill from CSV: {e}", exc_info=True)

    def _get_asset_token_mapping(self, assets: List[str]):
        """
        Get aToken and variableDebtToken addresses for given assets from ClickHouse.
        """
        if not assets:
            return {}

        assets_str = ",".join([f"'{asset}'" for asset in assets])
        query = f"""
        SELECT asset, aToken, variableDebtToken
        FROM aave_ethereum.view_LatestAssetConfiguration
        WHERE asset IN ({assets_str})
        """
        result = self.clickhouse_client.execute_query(query)
        return {
            row[0]: {"aToken": row[1], "variableDebtToken": row[2]}
            for row in result.result_rows
        }


BalancesBackfillTask = app.register_task(BalancesBackfillTask())
