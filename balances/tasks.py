import logging
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

                # Get collateral scaled balances in batches of 100
                collateral_balances = {}
                if atoken_address:
                    atoken = AaveToken(atoken_address)
                    for i in range(0, len(users), 100):
                        batch_users = users[i : i + 100]
                        batch_balances = atoken.get_scaled_balance(batch_users)
                        collateral_balances.update(batch_balances)

                # Get variable debt scaled balances in batches of 100
                debt_balances = {}
                if variable_debt_token_address:
                    debt_token = AaveToken(variable_debt_token_address)
                    for i in range(0, len(users), 100):
                        batch_users = users[i : i + 100]
                        batch_balances = debt_token.get_scaled_balance(batch_users)
                        debt_balances.update(batch_balances)

                # Prepare rows for ClickHouse insert (user, asset, collateral, debt, updated_at)
                for user in users:
                    collateral = collateral_balances.get(user, 0)
                    debt = debt_balances.get(user, 0)
                    # Note: updated_at will be set by DEFAULT now64() in ClickHouse
                    updates.append((user, asset, collateral, debt))

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
                                ],
                            )

                        self.clickhouse_client._execute_with_retry(insert_operation)
                        logger.info(
                            f"Successfully updated {len(updates)} scaled balances in LatestBalances_v2"
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
