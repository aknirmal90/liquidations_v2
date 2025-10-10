import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from celery import Task
from django.conf import settings
from django.core.cache import cache
from eth_utils import get_all_event_abis

from balances.models import BalanceEvent
from blockchains.models import Event
from liquidations_v2.celery_app import app
from oracles.models import PriceEvent
from utils.clickhouse.client import clickhouse_client
from utils.constants import (
    NETWORK_BLOCK_TIME,
    NETWORK_ID,
    NETWORK_NAME,
    PROTOCOL_ABI_PATH,
    PROTOCOL_CONFIG_PATH,
    PROTOCOL_NAME,
)
from utils.encoding import get_signature, get_topic_0
from utils.files import parse_json, parse_yaml
from utils.rpc import rpc_adapter
from utils.simulation import get_simulated_health_factor
from utils.tasks import EventSynchronizeMixin, ParentSynchronizeTaskMixin

logger = logging.getLogger(__name__)


class InitializeAppTask(Task):
    """Celery task to initialize the application by creating protocol events.

    This task reads the protocol configuration and ABI files to create or update
    Event instances in the database for each configured event. The events are
    associated with contract addresses and contain their ABI definitions.
    """

    def run(self):
        """Execute the initialization task.

        Creates protocol events by reading configuration files and updating the database.
        Logs the start and completion of the task.
        """
        logger.info(f"Starting InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}")
        clickhouse_client.create_database()
        self.create_protocol_events()
        self.create_materialized_views()
        logger.info(
            f"Completed InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}"
        )

    def create_materialized_views(self):
        """Create materialized views in Clickhouse."""
        BLOCKCHAINS_MATERIALIZED_VIEWS_PATH = os.path.join(
            os.path.dirname(settings.BASE_DIR), "blockchains", "mv_queries"
        )
        self._create_materialized_view_for_folder(BLOCKCHAINS_MATERIALIZED_VIEWS_PATH)

        ORACLES_MATERIALIZED_VIEWS_PATH = os.path.join(
            os.path.dirname(settings.BASE_DIR), "oracles", "mv_queries"
        )
        self._create_materialized_view_for_folder(ORACLES_MATERIALIZED_VIEWS_PATH)

        BALANCES_MATERIALIZED_VIEWS_PATH = os.path.join(
            os.path.dirname(settings.BASE_DIR), "balances", "mv_queries"
        )
        self._create_materialized_view_for_folder(BALANCES_MATERIALIZED_VIEWS_PATH)

    def _create_materialized_view_for_folder(self, folder: str):
        files = os.listdir(folder)
        files.sort()

        for filename in files:
            if not filename.endswith(".sql"):
                continue

            with open(os.path.join(folder, filename), "r") as file:
                query = file.read()
                logger.info(f"Executing query: {filename}")
                clickhouse_client.execute_query(query)

    def create_protocol_events(self):
        """Create or update Event instances for all configured protocol events.

        Reads the protocol configuration and ABI files to get contract addresses
        and event names. Creates or updates an Event instance for each configured
        event with its associated contract addresses and ABI definition.
        """
        protocol_config = parse_yaml(file_path=PROTOCOL_CONFIG_PATH)
        protocol_abi = parse_json(file_path=PROTOCOL_ABI_PATH)
        event_abis = get_all_event_abis(protocol_abi)

        contract_addresses = protocol_config.get("contract_addresses", [])
        contract_addresses = [address.lower() for address in contract_addresses]
        event_names = protocol_config.get("events", [])
        for event_name in event_names:
            event = self.create_or_get_event(
                event_abis=event_abis,
                event_name=event_name,
                contract_addresses=contract_addresses,
            )
            clickhouse_client.create_event_table(event)

    def create_or_get_event(
        self, event_abis: Dict[str, Any], event_name: str, contract_addresses: List[str]
    ):
        """Create or get an Event instance for the given event configuration.

        Args:
            event_abis (Dict[str, Any]): Dictionary of event ABIs from the protocol
            event_name (str): Name of the event to create/update
            contract_addresses (List[str]): List of contract addresses associated with the event

        Returns:
            Event: Created or existing Event instance

        Raises:
            Exception: If the ABI for the specified event name is not found
        """
        abi = None
        for event_abi in event_abis:
            if event_name == event_abi["name"]:
                abi = event_abi
                break

        if abi is None:
            raise Exception(f"ABI for event {event_name} not found")

        topic_0 = get_topic_0(abi)
        signature = get_signature(abi)

        event, is_created = Event.objects.update_or_create(
            name=event_name,
            defaults={
                "topic_0": topic_0,
                "signature": signature,
                "abi": abi,
                "contract_addresses": contract_addresses,
            },
        )

        if is_created:
            logger.info(
                f"Created Event instance: {event.name} for protocol "
                f"{PROTOCOL_NAME} on network {NETWORK_NAME}"
            )
        else:
            logger.info(
                f"Found existing Event instance: {event.name} for protocol "
                f"{PROTOCOL_NAME} on network {NETWORK_NAME}"
            )

        return event


InitializeAppTask = app.register_task(InitializeAppTask())


class ResetAppTask(Task):
    """Task to reset the app by deleting all Events, Networks and Protocols in reverse order."""

    def run(self):
        """Delete all Events, Networks and Protocols."""
        logger.info("Starting app reset...")
        # ResetAssetsTask.run()
        # Delete all Events first since they depend on Networks and Protocols
        clickhouse_client.drop_database()
        logger.info("Clickhouse Database dropped")

        events_count = Event.objects.all().count()
        Event.objects.all().delete()
        logger.info(f"Deleted {events_count} Events")

        price_events_count = PriceEvent.objects.all().count()
        PriceEvent.objects.all().delete()
        logger.info(f"Deleted {price_events_count} PriceEvents")

        balance_events_count = BalanceEvent.objects.all().count()
        BalanceEvent.objects.all().delete()
        logger.info(f"Deleted {balance_events_count} BalanceEvents")

        # Clear cache
        cache.clear()
        logger.info("Redis Cache cleared")

        logger.info("App reset complete")


ResetAppTask = app.register_task(ResetAppTask())


class ChildSynchronizeTask(EventSynchronizeMixin, Task):
    event_model = Event
    clickhouse_client = clickhouse_client
    rpc_adapter = rpc_adapter
    network_name = NETWORK_NAME


ChildSynchronizeTask = app.register_task(ChildSynchronizeTask())


class ParentSynchronizeTask(ParentSynchronizeTaskMixin, Task):
    event_model = Event
    child_task = ChildSynchronizeTask


ParentSynchronizeTask = app.register_task(ParentSynchronizeTask())


class UpdateNetworkBlockInfoTask(Task):
    """Task to update network block information in ClickHouse."""

    def run(self, block_number: int, block_timestamp: int):
        """Update network block info table with latest block data.

        Args:
            block_number (int): Latest block number
            block_timestamp (int): Latest block timestamp (Unix timestamp)
        """
        try:
            query = f"""
            INSERT INTO aave_ethereum.NetworkBlockInfo
            (network_id, latest_block_number, latest_block_timestamp, network_time_for_new_block)
            VALUES ({NETWORK_ID}, {block_number}, {block_timestamp * 1_000_000}, {NETWORK_BLOCK_TIME})
            """

            clickhouse_client.execute_query(query)
            clickhouse_client.execute_query(
                "SYSTEM RELOAD DICTIONARY aave_ethereum.NetworkBlockInfoDictionary"
            )

            logger.info(
                f"Updated NetworkBlockInfo for {NETWORK_NAME}: block {block_number}, timestamp {block_timestamp}"
            )

        except Exception as e:
            logger.error(f"Failed to update NetworkBlockInfo: {e}")
            raise


UpdateNetworkBlockInfoTask = app.register_task(UpdateNetworkBlockInfoTask())


class CalculateLiquidationHealthFactorMetricsTask(Task):
    """
    Task to calculate and store health factor metrics for liquidation events.

    This task processes liquidation events and calculates health factors at various
    transaction/block points for analysis. It implements locking to prevent concurrent
    execution and optimizes by skipping already processed transactions.
    """

    # Lock key for preventing concurrent execution
    LOCK_KEY = "liquidation_health_factor_calculation_lock"
    LOCK_TIMEOUT = 3600  # 1 hour lock timeout

    def run(self, limit: int = 500):
        """
        Calculate health factor metrics for the most recent liquidations.

        Args:
            limit (int): Number of most recent liquidations to process (default: 500)
        """
        logger.info(
            f"Starting CalculateLiquidationHealthFactorMetricsTask with limit={limit}"
        )

        # Try to acquire lock to prevent concurrent execution
        lock_acquired = cache.add(self.LOCK_KEY, "locked", self.LOCK_TIMEOUT)
        if not lock_acquired:
            logger.warning("Task already running, skipping execution")
            return {"status": "skipped", "reason": "task_already_running"}

        try:
            return self._process_liquidations(limit)
        finally:
            # Always release the lock
            cache.delete(self.LOCK_KEY)

    def _process_liquidations(self, limit: int) -> Dict[str, Any]:
        """
        Process liquidations and calculate health factor metrics.

        Args:
            limit (int): Number of liquidations to process

        Returns:
            Dict[str, Any]: Processing results summary
        """
        try:
            # Get recent liquidation events from ClickHouse that haven't been processed yet
            liquidations = self._get_unprocessed_liquidations(limit)

            if not liquidations:
                logger.info("No unprocessed liquidations found")
                return {
                    "status": "completed",
                    "processed_count": 0,
                    "total_liquidations": 0,
                    "errors": [],
                }

            logger.info(
                f"Found {len(liquidations)} unprocessed liquidations to process"
            )

            processed_count = 0
            errors = []

            for liquidation in liquidations:
                try:
                    self._calculate_health_factors_for_liquidation(liquidation)
                    processed_count += 1

                    if processed_count % 10 == 0:
                        logger.info(
                            f"Processed {processed_count}/{len(liquidations)} liquidations"
                        )

                except Exception as e:
                    error_msg = f"Failed to process liquidation {liquidation.get('transactionHash')}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append(error_msg)
                    continue

            logger.info(
                f"Completed processing: {processed_count} successful, {len(errors)} errors"
            )
            return {
                "status": "completed",
                "processed_count": processed_count,
                "total_liquidations": len(liquidations),
                "errors": errors,
            }

        except Exception as e:
            logger.error(
                f"Fatal error in _process_liquidations: {str(e)}", exc_info=True
            )
            return {"status": "error", "error": str(e)}

    def _get_unprocessed_liquidations(self, limit: int) -> List[Dict]:
        """
        Get recent liquidations that haven't been processed yet.

        Args:
            limit (int): Maximum number of liquidations to return

        Returns:
            List[Dict]: List of liquidation event data
        """
        query = f"""
        SELECT
            l.transactionHash,
            l.transactionIndex,
            l.logIndex,
            l.blockNumber,
            l.blockTimestamp,
            l.user,
            l.liquidator,
            l.collateralAsset,
            l.debtAsset,
            l.liquidatedCollateralAmount,
            l.debtToCover
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.LiquidationHealthFactorMetrics m
            ON l.transactionHash = m.transaction_hash
            AND l.logIndex = m.log_index
        WHERE m.transaction_hash = ''
        ORDER BY l.blockNumber DESC, l.transactionIndex DESC, l.logIndex DESC
        LIMIT {limit}
        """

        result = clickhouse_client.execute_query(query)

        # Convert result rows to dictionaries
        columns = [
            "transactionHash",
            "transactionIndex",
            "logIndex",
            "blockNumber",
            "blockTimestamp",
            "user",
            "liquidator",
            "collateralAsset",
            "debtAsset",
            "liquidatedCollateralAmount",
            "debtToCover",
        ]

        liquidations = []
        for row in result.result_rows:
            liquidation_dict = {}
            for i, col_name in enumerate(columns):
                liquidation_dict[col_name] = row[i]
            liquidations.append(liquidation_dict)

        return liquidations

    def _calculate_health_factors_for_liquidation(self, liquidation: Dict):
        """
        Calculate health factors at various points for a single liquidation.

        Args:
            liquidation (Dict): Liquidation event data
        """
        transaction_hash = liquidation["transactionHash"]
        block_number = liquidation["blockNumber"]
        transaction_index = liquidation["transactionIndex"]
        user_address = liquidation["user"]

        logger.debug(f"Calculating health factors for liquidation {transaction_hash}")

        # Define the points at which to calculate health factors
        calculation_points = [
            {
                "name": "health_factor_at_transaction",
                "block_number": block_number,
                "tx_index": transaction_index,
            },
            {
                "name": "health_factor_at_previous_tx",
                "block_number": block_number,
                "tx_index": max(0, transaction_index - 1),
            },
            {
                "name": "health_factor_at_block_start",
                "block_number": block_number,
                "tx_index": 0,
            },
            {
                "name": "health_factor_at_previous_block",
                "block_number": max(0, block_number - 1),
                "tx_index": 0,
            },
            {
                "name": "health_factor_at_two_blocks_prior",
                "block_number": max(0, block_number - 2),
                "tx_index": 0,
            },
        ]

        health_factors = {}
        calculation_errors = []

        # Calculate health factor at each point
        for point in calculation_points:
            try:
                health_factor = get_simulated_health_factor(
                    chain_id=NETWORK_ID,
                    address=user_address,
                    block_number=point["block_number"],
                    transaction_index=point["tx_index"],
                )
                health_factors[point["name"]] = health_factor
                logger.debug(f"Calculated {point['name']}: {health_factor}")

            except Exception as e:
                error_msg = f"Error calculating {point['name']}: {str(e)}"
                calculation_errors.append(error_msg)
                health_factors[point["name"]] = None
                logger.warning(error_msg)

        # Store the results in ClickHouse
        self._store_health_factor_metrics(
            liquidation, health_factors, calculation_errors
        )

    def _store_health_factor_metrics(
        self, liquidation: Dict, health_factors: Dict, errors: List[str]
    ):
        """
        Store calculated health factor metrics in ClickHouse.

        Args:
            liquidation (Dict): Original liquidation data
            health_factors (Dict): Calculated health factors
            errors (List[str]): Any calculation errors encountered
        """
        # Handle timestamp - it should already be a proper datetime from ClickHouse
        block_timestamp = liquidation["blockTimestamp"]
        if isinstance(block_timestamp, str):
            block_timestamp = datetime.fromisoformat(
                block_timestamp.replace("Z", "+00:00")
            )
        # If it's already a datetime object, use it as-is

        # Prepare data for insertion
        row_data = {
            "transaction_hash": liquidation["transactionHash"],
            "transaction_index": liquidation["transactionIndex"],
            "log_index": liquidation["logIndex"],
            "block_number": liquidation["blockNumber"],
            "block_timestamp": block_timestamp,
            "user_address": liquidation["user"],
            "liquidator_address": liquidation["liquidator"],
            "collateral_asset": liquidation["collateralAsset"],
            "debt_asset": liquidation["debtAsset"],
            "liquidated_collateral_amount": liquidation["liquidatedCollateralAmount"],
            "debt_to_cover": liquidation["debtToCover"],
            "health_factor_at_transaction": health_factors.get(
                "health_factor_at_transaction"
            ),
            "health_factor_at_previous_tx": health_factors.get(
                "health_factor_at_previous_tx"
            ),
            "health_factor_at_block_start": health_factors.get(
                "health_factor_at_block_start"
            ),
            "health_factor_at_previous_block": health_factors.get(
                "health_factor_at_previous_block"
            ),
            "health_factor_at_two_blocks_prior": health_factors.get(
                "health_factor_at_two_blocks_prior"
            ),
            "processed_at": datetime.now(timezone.utc),
            "calculation_errors": "; ".join(errors) if errors else "",
        }

        # Insert into ClickHouse using direct INSERT query
        try:
            # Debug logging
            logger.debug(
                f"Preparing to insert data for {liquidation['transactionHash']}"
            )

            # Use direct INSERT query instead of client.insert() method
            insert_query = """
            INSERT INTO aave_ethereum.LiquidationHealthFactorMetrics
            (transaction_hash, transaction_index, log_index, block_number, block_timestamp,
             user_address, liquidator_address, collateral_asset, debt_asset,
             liquidated_collateral_amount, debt_to_cover, health_factor_at_transaction,
             health_factor_at_previous_tx, health_factor_at_block_start,
             health_factor_at_previous_block, health_factor_at_two_blocks_prior,
             processed_at, calculation_errors)
            VALUES
            (%(transaction_hash)s, %(transaction_index)s, %(log_index)s, %(block_number)s, %(block_timestamp)s,
             %(user_address)s, %(liquidator_address)s, %(collateral_asset)s, %(debt_asset)s,
             %(liquidated_collateral_amount)s, %(debt_to_cover)s, %(health_factor_at_transaction)s,
             %(health_factor_at_previous_tx)s, %(health_factor_at_block_start)s,
             %(health_factor_at_previous_block)s, %(health_factor_at_two_blocks_prior)s,
             %(processed_at)s, %(calculation_errors)s)
            """

            clickhouse_client.execute_query(insert_query, parameters=row_data)
            logger.info(
                f"✅ Successfully stored health factor metrics for transaction {liquidation['transactionHash']}"
            )

        except Exception as e:
            logger.error(
                f"❌ Failed to store health factor metrics for {liquidation['transactionHash']}: {str(e)}"
            )
            logger.error(f"Row data: {row_data}")
            raise


CalculateLiquidationHealthFactorMetricsTask = app.register_task(
    CalculateLiquidationHealthFactorMetricsTask()
)
