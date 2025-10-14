import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from celery import Task
from django.conf import settings
from django.core.cache import cache
from eth_utils import get_all_event_abis

from balances.models import BalanceEvent
from blockchains.balance_test_tasks import (
    CompareCollateralBalanceTask,
    CompareDebtBalanceTask,
)
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


class CompareReserveConfigurationTask(Task):
    """
    Task to compare reserve configuration data between ClickHouse and RPC.

    This task retrieves reserve configuration from both ClickHouse (view_LatestAssetConfiguration)
    and from the DataProvider contract via RPC, compares matching fields, and stores
    aggregate statistics in the ReserveConfigurationTestResults table.
    """

    def run(self):
        """
        Execute the reserve configuration comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        import time

        logger.info("Starting CompareReserveConfigurationTask")
        start_time = time.time()

        try:
            # Get data from ClickHouse
            clickhouse_data = self._get_clickhouse_reserves()
            clickhouse_count = len(clickhouse_data)
            logger.info(f"Retrieved {clickhouse_count} reserves from ClickHouse")

            # Get all aTokens from RPC to compare counts
            all_atokens = self._get_all_atokens_count()
            logger.info(f"Retrieved {all_atokens} aTokens from getAllATokens()")

            # Check if counts match
            count_mismatch = clickhouse_count != all_atokens
            if count_mismatch:
                self._send_count_mismatch_notification(clickhouse_count, all_atokens)

            # Get data from RPC
            asset_addresses = list(clickhouse_data.keys())
            rpc_data = self._get_rpc_reserves(asset_addresses)
            logger.info(f"Retrieved {len(rpc_data)} reserves from RPC")

            # Compare the data
            comparison_results = self._compare_reserves(clickhouse_data, rpc_data)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration)

            # Clean up old test records (older than 7 days)
            self._cleanup_old_test_records()

            logger.info(f"Comparison completed in {test_duration:.2f} seconds")
            logger.info(
                f"Match percentage: {comparison_results['match_percentage']:.2f}%"
            )

            # Send notification if mismatches found
            if comparison_results["mismatched_records"] > 0:
                self._send_mismatch_notification(comparison_results)

            return {
                "status": "completed",
                "test_duration": test_duration,
                "count_mismatch": count_mismatch,
                "clickhouse_count": clickhouse_count,
                "rpc_atokens_count": all_atokens,
                **comparison_results,
            }

        except Exception as e:
            error_msg = f"Error during reserve configuration comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_clickhouse_reserves(self) -> Dict[str, Dict]:
        """
        Get reserve configuration data from ClickHouse.

        Returns:
            Dict[str, Dict]: Reserve data keyed by asset address
        """
        query = """
        SELECT
            asset,
            aToken,
            variableDebtToken,
            interestRateStrategyAddress,
            collateralLTV,
            collateralLiquidationThreshold,
            collateralLiquidationBonus,
            eModeCategoryId,
            eModeLTV,
            eModeLiquidationThreshold,
            eModeLiquidationBonus
        FROM aave_ethereum.view_LatestAssetConfiguration
        ORDER BY asset
        """

        result = clickhouse_client.execute_query(query)

        reserves = {}
        for row in result.result_rows:
            asset = row[0].lower()
            reserves[asset] = {
                "asset": asset,
                "aToken": row[1].lower() if row[1] else "",
                "variableDebtToken": row[2].lower() if row[2] else "",
                "interestRateStrategyAddress": row[3].lower() if row[3] else "",
                "ltv": float(row[4]) if row[4] is not None else 0,
                "liquidationThreshold": float(row[5]) if row[5] is not None else 0,
                "liquidationBonus": float(row[6]) if row[6] is not None else 0,
                "eModeCategoryId": int(row[7]) if row[7] is not None else 0,
                "eModeLtv": float(row[8]) if row[8] is not None else 0,
                "eModeLiquidationThreshold": float(row[9]) if row[9] is not None else 0,
                "eModeLiquidationBonus": float(row[10]) if row[10] is not None else 0,
            }

        return reserves

    def _get_rpc_reserves(self, asset_addresses: List[str]) -> Dict[str, Dict]:
        """
        Get reserve configuration data from RPC via DataProvider interface.

        Args:
            asset_addresses: List of asset addresses to query

        Returns:
            Dict[str, Dict]: Reserve data keyed by asset address
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()
        rpc_results = data_provider.get_reserves_configuration(asset_addresses)

        reserves = {}
        for asset, config in rpc_results.items():
            asset_lower = asset.lower()

            # The RPC returns a dict with field names as keys
            # decimals, ltv, liquidationThreshold, liquidationBonus, reserveFactor,
            # usageAsCollateralEnabled, borrowingEnabled, stableBorrowRateEnabled, isActive, isFrozen
            reserves[asset_lower] = {
                "asset": asset_lower,
                "ltv": float(config.get("ltv", 0))
                if config.get("ltv")
                else 0,  # Convert basis points to percentage
                "liquidationThreshold": float(config.get("liquidationThreshold", 0))
                if config.get("liquidationThreshold")
                else 0,
                "liquidationBonus": float(config.get("liquidationBonus", 0))
                if config.get("liquidationBonus")
                else 0,
                "decimals": int(config.get("decimals", 0))
                if config.get("decimals")
                else 0,
                "usageAsCollateralEnabled": bool(
                    config.get("usageAsCollateralEnabled", False)
                ),
                "borrowingEnabled": bool(config.get("borrowingEnabled", False)),
                "isActive": bool(config.get("isActive", False)),
                "isFrozen": bool(config.get("isFrozen", False)),
            }

        return reserves

    def _compare_reserves(
        self, clickhouse_data: Dict, rpc_data: Dict
    ) -> Dict[str, Any]:
        """
        Compare reserve data from ClickHouse and RPC.

        Args:
            clickhouse_data: Reserve data from ClickHouse
            rpc_data: Reserve data from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_assets = set(clickhouse_data.keys())
        rpc_assets = set(rpc_data.keys())

        common_assets = clickhouse_assets & rpc_assets
        clickhouse_only = clickhouse_assets - rpc_assets
        rpc_only = rpc_assets - clickhouse_assets

        matching_count = 0
        mismatched_count = 0
        mismatches = []

        # Compare common assets
        for asset in common_assets:
            ch_reserve = clickhouse_data[asset]
            rpc_reserve = rpc_data[asset]

            # Compare the fields that exist in both datasets
            fields_to_compare = ["ltv", "liquidationThreshold", "liquidationBonus"]

            is_match = True
            asset_mismatches = []

            for field in fields_to_compare:
                if field in ch_reserve and field in rpc_reserve:
                    ch_value = ch_reserve[field]
                    rpc_value = rpc_reserve[field]

                    # Allow small floating point differences (within 0.01%)
                    if ch_value != rpc_value:
                        is_match = False
                        asset_mismatches.append(
                            f"{field}: CH={ch_value:.2f} RPC={rpc_value:.2f}"
                        )

            if is_match:
                matching_count += 1
            else:
                mismatched_count += 1
                mismatches.append(f"{asset}: {', '.join(asset_mismatches)}")

        total_reserves = len(clickhouse_assets | rpc_assets)
        match_percentage = (
            (matching_count / len(common_assets) * 100) if common_assets else 0
        )

        return {
            "total_reserves": total_reserves,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "clickhouse_only_records": len(clickhouse_only),
            "rpc_only_records": len(rpc_only),
            "match_percentage": match_percentage,
            "mismatches_detail": "; ".join(
                mismatches[:50]
            ),  # Limit to first 50 mismatches
        }

    def _store_test_results(self, results: Dict[str, Any], duration: float):
        """
        Store test results in ClickHouse.

        Args:
            results: Comparison results
            duration: Test duration in seconds
        """
        query = """
        INSERT INTO aave_ethereum.ReserveConfigurationTestResults
        (test_timestamp, total_reserves, matching_records, mismatched_records,
         clickhouse_only_records, rpc_only_records, match_percentage,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(total_reserves)s, %(matching_records)s, %(mismatched_records)s,
         %(clickhouse_only_records)s, %(rpc_only_records)s, %(match_percentage)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "total_reserves": results["total_reserves"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "clickhouse_only_records": results["clickhouse_only_records"],
            "rpc_only_records": results["rpc_only_records"],
            "match_percentage": results["match_percentage"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info("Test results stored successfully in ClickHouse")

    def _store_error_result(self, error_message: str, duration: float):
        """
        Store error result in ClickHouse.

        Args:
            error_message: Error message
            duration: Test duration before error
        """
        query = """
        INSERT INTO aave_ethereum.ReserveConfigurationTestResults
        (test_timestamp, total_reserves, matching_records, mismatched_records,
         clickhouse_only_records, rpc_only_records, match_percentage,
         test_duration_seconds, test_status, error_message)
        VALUES
        (now64(), 0, 0, 0, 0, 0, 0, %(test_duration_seconds)s, 'error', %(error_message)s)
        """

        parameters = {"test_duration_seconds": duration, "error_message": error_message}

        try:
            clickhouse_client.execute_query(query, parameters=parameters)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """
        Send a push notification when mismatches are found.

        Args:
            results: Comparison results containing mismatch information
        """
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_reserves"]
            match_percentage = results["match_percentage"]

            title = "⚠️ Reserve Configuration Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} reserves.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"ClickHouse only: {results['clickhouse_only_records']}\n"
                f"RPC only: {results['rpc_only_records']}"
            )

            send_simplepush_notification(
                title=title, message=message, event="reserve_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} mismatches found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")
            # Don't raise - notification failure shouldn't fail the task

    def _cleanup_old_test_records(self):
        """
        Delete test records older than 7 days from ClickHouse.
        """
        try:
            query = """
            ALTER TABLE aave_ethereum.ReserveConfigurationTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")
            # Don't raise - cleanup failure shouldn't fail the task

    def _get_all_atokens_count(self) -> int:
        """
        Get the count of all aTokens from the DataProvider contract.

        Returns:
            int: Number of aTokens returned by getAllATokens()
        """
        try:
            from utils.interfaces.dataprovider import DataProviderInterface

            data_provider = DataProviderInterface()
            all_atokens = data_provider.get_all_atokens()
            return len(all_atokens)

        except Exception as e:
            logger.error(f"Failed to get aTokens count from RPC: {e}")
            return 0

    def _send_count_mismatch_notification(self, clickhouse_count: int, rpc_count: int):
        """
        Send a push notification when reserve counts don't match.

        Args:
            clickhouse_count: Number of reserves in ClickHouse
            rpc_count: Number of aTokens from getAllATokens()
        """
        try:
            from utils.simplepush import send_simplepush_notification

            difference = abs(clickhouse_count - rpc_count)
            title = "🔢 Reserve Count Mismatch Detected"
            message = (
                f"Reserve count mismatch detected!\n"
                f"ClickHouse: {clickhouse_count} reserves\n"
                f"RPC getAllATokens: {rpc_count} aTokens\n"
                f"Difference: {difference}"
            )

            send_simplepush_notification(
                title=title, message=message, event="reserve_count_mismatch"
            )

            logger.info(
                f"Sent count mismatch notification: {clickhouse_count} vs {rpc_count}"
            )

        except Exception as e:
            logger.error(f"Failed to send count mismatch notification: {e}")
            # Don't raise - notification failure shouldn't fail the task


CompareReserveConfigurationTask = app.register_task(CompareReserveConfigurationTask())


class CompareUserEModeTask(Task):
    """
    Task to compare user eMode status between ClickHouse and RPC.

    This task retrieves users who have modified their eMode status since the last test run
    (or all users if no previous run), queries the Pool contract via RPC to get their
    current eMode status, and compares with the EModeStatusDictionary in ClickHouse.
    """

    def run(self):
        """
        Execute the user eMode comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        import time

        logger.info("Starting CompareUserEModeTask")
        start_time = time.time()

        try:
            # Get the timestamp of the last test run
            last_test_timestamp = self._get_last_test_timestamp()

            # Get users to test
            users_to_test = self._get_users_to_test(last_test_timestamp)
            total_users = len(users_to_test)
            logger.info(
                f"Retrieved {total_users} users to test "
                f"(since {last_test_timestamp or 'beginning'})"
            )

            if total_users == 0:
                logger.info("No users to test")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_users": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            # Get ClickHouse eMode status for these users
            clickhouse_data = self._get_clickhouse_emode_status(users_to_test)
            logger.info(
                f"Retrieved {len(clickhouse_data)} user eMode statuses from ClickHouse"
            )

            # Get RPC eMode data in batches of 100
            rpc_data = self._get_rpc_emode_status_batched(users_to_test, batch_size=100)
            logger.info(f"Retrieved {len(rpc_data)} user eMode statuses from RPC")

            # Compare the data
            comparison_results = self._compare_emode_status(clickhouse_data, rpc_data)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration)

            # Clean up old test records (older than 7 days)
            self._cleanup_old_test_records()

            logger.info(f"Comparison completed in {test_duration:.2f} seconds")
            logger.info(
                f"Match percentage: {comparison_results['match_percentage']:.2f}%"
            )

            # Send notification if mismatches found
            if comparison_results["mismatched_records"] > 0:
                self._send_mismatch_notification(comparison_results)

            return {
                "status": "completed",
                "test_duration": test_duration,
                **comparison_results,
            }

        except Exception as e:
            error_msg = f"Error during user eMode comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_last_test_timestamp(self):
        """
        Get the timestamp of the last successful test run.

        Returns:
            str or None: Last test timestamp or None if no previous test
        """
        query = """
        SELECT test_timestamp
        FROM aave_ethereum.UserEModeTestResults
        WHERE test_status = 'completed'
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        result = clickhouse_client.execute_query(query)

        if result.result_rows:
            return result.result_rows[0][0]
        return None

    def _get_users_to_test(self, since_timestamp) -> List[str]:
        """
        Get list of users who have modified their eMode status since the given timestamp.
        If no timestamp is provided, returns all unique users.

        Args:
            since_timestamp: Timestamp to filter users, or None for all users

        Returns:
            List[str]: List of user addresses
        """
        if since_timestamp:
            query = """
            SELECT DISTINCT user
            FROM aave_ethereum.UserEModeSet
            WHERE blockTimestamp > %(since_timestamp)s
            ORDER BY user
            """
            parameters = {"since_timestamp": since_timestamp}
        else:
            query = """
            SELECT DISTINCT user
            FROM aave_ethereum.UserEModeSet
            ORDER BY user
            """
            parameters = {}

        result = clickhouse_client.execute_query(query, parameters=parameters)

        return [row[0] for row in result.result_rows]

    def _get_clickhouse_emode_status(self, users: List[str]) -> Dict[str, bool]:
        """
        Get eMode enabled status from ClickHouse for the given users.
        Processes users in batches to avoid exceeding max_query_size.

        Args:
            users: List of user addresses

        Returns:
            Dict[str, bool]: Mapping of user address to eMode enabled status
        """
        if not users:
            return {}

        emode_status = {}
        batch_size = 1000  # Process 1000 users at a time to avoid query size limits

        # Process users in batches
        for i in range(0, len(users), batch_size):
            batch = users[i : i + batch_size]
            logger.info(
                f"Querying ClickHouse for eMode status batch {i // batch_size + 1} "
                f"({len(batch)} users)"
            )

            # Use EModeStatusDictionary with FINAL to get latest status
            # ReplacingMergeTree requires FINAL to get the most recent version
            query = """
            SELECT user, is_enabled_in_emode
            FROM aave_ethereum.EModeStatusDictionary FINAL
            WHERE user IN %(users)s
            """

            parameters = {"users": batch}
            result = clickhouse_client.execute_query(query, parameters=parameters)

            for row in result.result_rows:
                user = row[0].lower()
                is_enabled = bool(row[1])
                emode_status[user] = is_enabled

        return emode_status

    def _get_rpc_emode_status_batched(
        self, users: List[str], batch_size: int = 100
    ) -> Dict[str, bool]:
        """
        Get eMode enabled status from RPC in batches.

        Args:
            users: List of user addresses
            batch_size: Number of users to query per batch

        Returns:
            Dict[str, bool]: Mapping of user address to eMode enabled status
        """
        from utils.interfaces.pool import PoolInterface

        pool = PoolInterface()
        emode_status = {}

        # Process users in batches
        for i in range(0, len(users), batch_size):
            batch = users[i : i + batch_size]
            logger.info(
                f"Querying eMode for batch {i // batch_size + 1} ({len(batch)} users)"
            )

            try:
                batch_results = pool.get_user_emode(batch)

                # Convert category ID to boolean (0 = disabled, >0 = enabled)
                for user, category_id in batch_results.items():
                    user_lower = user.lower()
                    emode_status[user_lower] = category_id > 0

            except Exception as e:
                logger.error(f"Error querying batch starting at index {i}: {e}")
                raise

        return emode_status

    def _compare_emode_status(
        self, clickhouse_data: Dict[str, bool], rpc_data: Dict[str, bool]
    ) -> Dict[str, Any]:
        """
        Compare eMode status from ClickHouse and RPC.

        Args:
            clickhouse_data: eMode status from ClickHouse
            rpc_data: eMode status from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_users = set(clickhouse_data.keys())
        rpc_users = set(rpc_data.keys())

        common_users = clickhouse_users & rpc_users
        clickhouse_only = clickhouse_users - rpc_users
        rpc_only = rpc_users - clickhouse_users

        matching_count = 0
        mismatched_count = 0
        mismatches = []

        # Compare common users
        for user in common_users:
            ch_enabled = clickhouse_data[user]
            rpc_enabled = rpc_data[user]

            if ch_enabled == rpc_enabled:
                matching_count += 1
            else:
                mismatched_count += 1
                mismatches.append(
                    f"{user}: CH={'enabled' if ch_enabled else 'disabled'} "
                    f"RPC={'enabled' if rpc_enabled else 'disabled'}"
                )

        total_users = len(clickhouse_users | rpc_users)
        match_percentage = (
            (matching_count / len(common_users) * 100) if common_users else 0
        )

        return {
            "total_users": total_users,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "clickhouse_only_records": len(clickhouse_only),
            "rpc_only_records": len(rpc_only),
            "match_percentage": match_percentage,
            "mismatches_detail": "; ".join(
                mismatches[:50]
            ),  # Limit to first 50 mismatches
        }

    def _store_test_results(self, results: Dict[str, Any], duration: float):
        """
        Store test results in ClickHouse.

        Args:
            results: Comparison results
            duration: Test duration in seconds
        """
        query = """
        INSERT INTO aave_ethereum.UserEModeTestResults
        (test_timestamp, total_users, matching_records, mismatched_records,
         clickhouse_only_records, rpc_only_records, match_percentage,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(total_users)s, %(matching_records)s, %(mismatched_records)s,
         %(clickhouse_only_records)s, %(rpc_only_records)s, %(match_percentage)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "total_users": results["total_users"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "clickhouse_only_records": results["clickhouse_only_records"],
            "rpc_only_records": results["rpc_only_records"],
            "match_percentage": results["match_percentage"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info("Test results stored successfully in ClickHouse")

    def _store_error_result(self, error_message: str, duration: float):
        """
        Store error result in ClickHouse.

        Args:
            error_message: Error message
            duration: Test duration before error
        """
        query = """
        INSERT INTO aave_ethereum.UserEModeTestResults
        (test_timestamp, total_users, matching_records, mismatched_records,
         clickhouse_only_records, rpc_only_records, match_percentage,
         test_duration_seconds, test_status, error_message)
        VALUES
        (now64(), 0, 0, 0, 0, 0, 0, %(test_duration_seconds)s, 'error', %(error_message)s)
        """

        parameters = {"test_duration_seconds": duration, "error_message": error_message}

        try:
            clickhouse_client.execute_query(query, parameters=parameters)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """
        Send a push notification when mismatches are found.

        Args:
            results: Comparison results containing mismatch information
        """
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_users"]
            match_percentage = results["match_percentage"]

            title = "⚠️ User eMode Status Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} users.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"ClickHouse only: {results['clickhouse_only_records']}\n"
                f"RPC only: {results['rpc_only_records']}"
            )

            send_simplepush_notification(
                title=title, message=message, event="user_emode_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} mismatches found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")
            # Don't raise - notification failure shouldn't fail the task

    def _cleanup_old_test_records(self):
        """
        Delete test records older than 7 days from ClickHouse.
        """
        try:
            query = """
            ALTER TABLE aave_ethereum.UserEModeTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")
            # Don't raise - cleanup failure shouldn't fail the task


CompareUserEModeTask = app.register_task(CompareUserEModeTask())


class CompareUserCollateralTask(Task):
    """
    Task to compare user collateral status between ClickHouse and RPC.

    This task retrieves user-asset pairs that have modified collateral status since
    the last test run (or all pairs if no previous run), queries the DataProvider
    contract via RPC to get their current collateral enabled status, and compares
    with the CollateralStatusDictionary in ClickHouse.
    """

    def run(self):
        """
        Execute the user collateral status comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        import time

        logger.info("Starting CompareUserCollateralTask")
        start_time = time.time()

        try:
            # Get the timestamp of the last test run
            last_test_timestamp = self._get_last_test_timestamp()

            # Get user-asset pairs to test
            user_asset_pairs = self._get_user_asset_pairs_to_test(last_test_timestamp)
            total_pairs = len(user_asset_pairs)
            logger.info(
                f"Retrieved {total_pairs} user-asset pairs to test "
                f"(since {last_test_timestamp or 'beginning'})"
            )

            if total_pairs == 0:
                logger.info("No user-asset pairs to test")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_user_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            # Get ClickHouse collateral status for these pairs
            clickhouse_data = self._get_clickhouse_collateral_status(user_asset_pairs)
            logger.info(
                f"Retrieved {len(clickhouse_data)} user-asset collateral statuses from ClickHouse"
            )

            # Get RPC collateral data in batches of 100
            rpc_data = self._get_rpc_collateral_status_batched(
                user_asset_pairs, batch_size=100
            )
            logger.info(
                f"Retrieved {len(rpc_data)} user-asset collateral statuses from RPC"
            )

            # Compare the data
            comparison_results = self._compare_collateral_status(
                clickhouse_data, rpc_data
            )

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration)

            # Clean up old test records (older than 7 days)
            self._cleanup_old_test_records()

            logger.info(f"Comparison completed in {test_duration:.2f} seconds")
            logger.info(
                f"Match percentage: {comparison_results['match_percentage']:.2f}%"
            )

            # Send notification if mismatches found
            if comparison_results["mismatched_records"] > 0:
                self._send_mismatch_notification(comparison_results)

            return {
                "status": "completed",
                "test_duration": test_duration,
                **comparison_results,
            }

        except Exception as e:
            error_msg = f"Error during user collateral comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_last_test_timestamp(self):
        """
        Get the timestamp of the last successful test run.

        Returns:
            DateTime or None: Last test timestamp or None if no previous test
        """
        query = """
        SELECT test_timestamp
        FROM aave_ethereum.UserCollateralTestResults
        WHERE test_status = 'completed'
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        result = clickhouse_client.execute_query(query)

        if result.result_rows:
            return result.result_rows[0][0]
        return None

    def _get_user_asset_pairs_to_test(self, since_timestamp) -> List[tuple]:
        """
        Get list of (user, asset) pairs that have modified collateral status
        since the given timestamp. If no timestamp, returns all unique pairs.

        Args:
            since_timestamp: Timestamp to filter pairs, or None for all pairs

        Returns:
            List[tuple]: List of (user, asset) tuples
        """
        # Use a simpler approach with subquery to ensure proper type handling
        if since_timestamp:
            query = """
            SELECT user, asset
            FROM (
                SELECT toString(user) as user, toString(reserve) as asset
                FROM aave_ethereum.ReserveUsedAsCollateralEnabled
                WHERE blockTimestamp > %(since_timestamp)s
                UNION ALL
                SELECT toString(user) as user, toString(reserve) as asset
                FROM aave_ethereum.ReserveUsedAsCollateralDisabled
                WHERE blockTimestamp > %(since_timestamp)s
            ) AS combined
            GROUP BY user, asset
            """
            parameters = {"since_timestamp": since_timestamp}
        else:
            query = """
            SELECT user, asset
            FROM (
                SELECT toString(user) as user, toString(reserve) as asset
                FROM aave_ethereum.ReserveUsedAsCollateralEnabled
                UNION ALL
                SELECT toString(user) as user, toString(reserve) as asset
                FROM aave_ethereum.ReserveUsedAsCollateralDisabled
            ) AS combined
            GROUP BY user, asset
            """
            parameters = {}

        try:
            result = clickhouse_client.execute_query(query, parameters=parameters)

            if not result or not result.result_rows:
                logger.warning(
                    "No user-asset pairs found in ReserveUsedAsCollateral events"
                )
                return []

            pairs = [(str(row[0]), str(row[1])) for row in result.result_rows]
            logger.info(f"Found {len(pairs)} unique user-asset pairs")
            return pairs

        except Exception as e:
            logger.error(f"Error fetching user-asset pairs: {e}", exc_info=True)
            raise

    def _get_clickhouse_collateral_status(
        self, user_asset_pairs: List[tuple]
    ) -> Dict[tuple, bool]:
        """
        Get collateral enabled status from ClickHouse for the given pairs.
        Queries in small batches with argMax for efficiency.

        Args:
            user_asset_pairs: List of (user, asset) tuples

        Returns:
            Dict[tuple, bool]: Mapping of (user, asset) to collateral enabled status
        """
        if not user_asset_pairs:
            return {}

        logger.info(
            f"Querying ClickHouse for collateral status of {len(user_asset_pairs)} pairs"
        )

        collateral_status = {}
        batch_size = 500  # Process 500 pairs at a time

        # Process in small batches to avoid timeouts
        for i in range(0, len(user_asset_pairs), batch_size):
            batch = user_asset_pairs[i : i + batch_size]
            logger.info(
                f"Querying ClickHouse batch {i // batch_size + 1}/{(len(user_asset_pairs) + batch_size - 1) // batch_size} "
                f"({len(batch)} pairs)"
            )

            # Get unique users and assets for this batch
            users_in_batch = list(set([user for user, _ in batch]))
            assets_in_batch = list(set([asset for _, asset in batch]))

            # Query with argMax instead of FINAL - much faster and less memory
            # Use toString() for addresses to avoid type interpretation issues
            query = """
            SELECT
                toString(user) as user,
                toString(asset) as asset,
                argMax(is_enabled_as_collateral, version) as is_enabled_as_collateral
            FROM aave_ethereum.CollateralStatusDictionary
            WHERE user IN %(users)s AND asset IN %(assets)s
            GROUP BY user, asset
            """

            parameters = {"users": users_in_batch, "assets": assets_in_batch}
            result = clickhouse_client.execute_query(query, parameters=parameters)

            # Filter to only include requested pairs
            batch_set = set(batch)
            for row in result.result_rows:
                user = row[0].lower()
                asset = row[1].lower()
                pair = (user, asset)

                # Only include if this pair was actually requested
                if pair in batch_set or (row[0], row[1]) in batch_set:
                    is_enabled = bool(row[2])
                    collateral_status[pair] = is_enabled

        logger.info(
            f"Retrieved {len(collateral_status)} collateral statuses from ClickHouse"
        )

        return collateral_status

    def _get_rpc_collateral_status_batched(
        self, user_asset_pairs: List[tuple], batch_size: int = 100
    ) -> Dict[tuple, bool]:
        """
        Get collateral enabled status from RPC in batches.

        Args:
            user_asset_pairs: List of (user, asset) tuples
            batch_size: Number of pairs to query per batch

        Returns:
            Dict[tuple, bool]: Mapping of (user, asset) to collateral enabled status
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()
        collateral_status = {}

        # Process pairs in batches
        for i in range(0, len(user_asset_pairs), batch_size):
            batch = user_asset_pairs[i : i + batch_size]
            logger.info(
                f"Querying RPC for collateral status batch {i // batch_size + 1} "
                f"({len(batch)} pairs)"
            )

            try:
                batch_results = data_provider.get_user_reserve_data(batch)

                # Extract usageAsCollateralEnabled from results
                for (user, asset), result in batch_results.items():
                    user_lower = user.lower()
                    asset_lower = asset.lower()
                    # The result dict has usageAsCollateralEnabled field
                    is_enabled = result.get("usageAsCollateralEnabled", False)
                    collateral_status[(user_lower, asset_lower)] = bool(is_enabled)

            except Exception as e:
                logger.error(f"Error querying batch starting at index {i}: {e}")
                raise

        return collateral_status

    def _compare_collateral_status(
        self, clickhouse_data: Dict[tuple, bool], rpc_data: Dict[tuple, bool]
    ) -> Dict[str, Any]:
        """
        Compare collateral status from ClickHouse and RPC.

        Args:
            clickhouse_data: Collateral status from ClickHouse
            rpc_data: Collateral status from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_pairs = set(clickhouse_data.keys())
        rpc_pairs = set(rpc_data.keys())

        common_pairs = clickhouse_pairs & rpc_pairs
        clickhouse_only = clickhouse_pairs - rpc_pairs
        rpc_only = rpc_pairs - clickhouse_pairs

        matching_count = 0
        mismatched_count = 0
        mismatches = []

        # Compare common pairs
        for pair in common_pairs:
            ch_enabled = clickhouse_data[pair]
            rpc_enabled = rpc_data[pair]

            if ch_enabled == rpc_enabled:
                matching_count += 1
            else:
                mismatched_count += 1
                user, asset = pair
                mismatches.append(
                    f"({user},{asset}): CH={'enabled' if ch_enabled else 'disabled'} "
                    f"RPC={'enabled' if rpc_enabled else 'disabled'}"
                )

        total_pairs = len(clickhouse_pairs | rpc_pairs)
        match_percentage = (
            (matching_count / len(common_pairs) * 100) if common_pairs else 0
        )

        return {
            "total_user_assets": total_pairs,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "clickhouse_only_records": len(clickhouse_only),
            "rpc_only_records": len(rpc_only),
            "match_percentage": match_percentage,
            "mismatches_detail": "; ".join(
                mismatches[:50]
            ),  # Limit to first 50 mismatches
        }

    def _store_test_results(self, results: Dict[str, Any], duration: float):
        """
        Store test results in ClickHouse.

        Args:
            results: Comparison results
            duration: Test duration in seconds
        """
        query = """
        INSERT INTO aave_ethereum.UserCollateralTestResults
        (test_timestamp, total_user_assets, matching_records, mismatched_records,
         clickhouse_only_records, rpc_only_records, match_percentage,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(total_user_assets)s, %(matching_records)s, %(mismatched_records)s,
         %(clickhouse_only_records)s, %(rpc_only_records)s, %(match_percentage)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "total_user_assets": results["total_user_assets"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "clickhouse_only_records": results["clickhouse_only_records"],
            "rpc_only_records": results["rpc_only_records"],
            "match_percentage": results["match_percentage"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info("Test results stored successfully in ClickHouse")

    def _store_error_result(self, error_message: str, duration: float):
        """
        Store error result in ClickHouse.

        Args:
            error_message: Error message
            duration: Test duration before error
        """
        query = """
        INSERT INTO aave_ethereum.UserCollateralTestResults
        (test_timestamp, total_user_assets, matching_records, mismatched_records,
         clickhouse_only_records, rpc_only_records, match_percentage,
         test_duration_seconds, test_status, error_message)
        VALUES
        (now64(), 0, 0, 0, 0, 0, 0, %(test_duration_seconds)s, 'error', %(error_message)s)
        """

        parameters = {"test_duration_seconds": duration, "error_message": error_message}

        try:
            clickhouse_client.execute_query(query, parameters=parameters)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """
        Send a push notification when mismatches are found.

        Args:
            results: Comparison results containing mismatch information
        """
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_user_assets"]
            match_percentage = results["match_percentage"]

            title = "⚠️ User Collateral Status Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} user-asset pairs.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"ClickHouse only: {results['clickhouse_only_records']}\n"
                f"RPC only: {results['rpc_only_records']}"
            )

            send_simplepush_notification(
                title=title, message=message, event="user_collateral_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} mismatches found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")
            # Don't raise - notification failure shouldn't fail the task

    def _cleanup_old_test_records(self):
        """
        Delete test records older than 7 days from ClickHouse.
        """
        try:
            query = """
            ALTER TABLE aave_ethereum.UserCollateralTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")
            # Don't raise - cleanup failure shouldn't fail the task


CompareUserCollateralTask = app.register_task(CompareUserCollateralTask())

# Register balance test tasks
compare_collateral_balance_task = app.register_task(CompareCollateralBalanceTask())
compare_debt_balance_task = app.register_task(CompareDebtBalanceTask())
