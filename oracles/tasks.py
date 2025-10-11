import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import web3
from celery import Task
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from eth_utils import get_all_event_abis
from web3.datastructures import AttributeDict

from balances.models import BalanceEvent
from liquidations_v2.celery_app import app
from oracles.contracts.denominator import get_denominator
from oracles.contracts.interface import PriceOracleInterface
from oracles.contracts.max_cap import get_max_cap
from oracles.contracts.multiplier import get_multiplier
from oracles.contracts.numerator import get_numerator
from oracles.contracts.underlying_sources import get_underlying_sources
from oracles.contracts.utils import (
    CACHE_TTL_4_HOURS,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
    get_latest_asset_sources,
)
from oracles.models import PriceEvent
from utils.clickhouse.client import clickhouse_client
from utils.constants import (
    NETWORK_NAME,
    PRICES_ABI_PATH,
    PROTOCOL_ABI_PATH,
    PROTOCOL_NAME,
)
from utils.encoding import get_signature, get_topic_0
from utils.files import parse_json
from utils.rpc import rpc_adapter
from utils.tasks import EventSynchronizeMixin, ParentSynchronizeTaskMixin
from utils.tokens import Token

logger = logging.getLogger(__name__)


class InitializePriceEvents(Task):
    def get_price_events_cache_key(self, asset: str, asset_source: str) -> str:
        return f"{PROTOCOL_NAME}-{NETWORK_NAME}-{asset}-{asset_source}"

    def _create_mint_burn_transfer_balance_events(
        self,
        asset_token_address_pairs: List[Tuple[str, str]],
        event_abis: Dict[str, Any],
        type: BalanceEvent.BalanceType,
    ):
        for asset, aToken in asset_token_address_pairs:
            self._create_or_get_balance_event(
                event_abis=event_abis,
                event_name="BalanceTransfer",
                contract_addresses=[aToken],
                asset=asset,
                type=type,
            )
            self._create_or_get_balance_event(
                event_abis=event_abis,
                event_name="Mint",
                contract_addresses=[aToken],
                asset=asset,
                type=type,
            )
            self._create_or_get_balance_event(
                event_abis=event_abis,
                event_name="Burn",
                contract_addresses=[aToken],
                asset=asset,
                type=type,
            )

    def create_balance_events(self):
        reserve_configurations = clickhouse_client.select_rows(
            "view_LatestAssetConfiguration"
        )
        aTokens = [
            (row[0], row[1])
            for row in reserve_configurations
            if row[1] != "0x0000000000000000000000000000000000000000"
        ]
        stableDebtTokens = [
            (row[0], row[2])
            for row in reserve_configurations
            if row[2] != "0x0000000000000000000000000000000000000000"
        ]
        variableDebtTokens = [
            (row[0], row[3])
            for row in reserve_configurations
            if row[3] != "0x0000000000000000000000000000000000000000"
        ]

        all_abis = parse_json(file_path=PROTOCOL_ABI_PATH)
        all_events_abis = get_all_event_abis(all_abis)

        self._create_mint_burn_transfer_balance_events(
            asset_token_address_pairs=aTokens,
            event_abis=all_events_abis,
            type=BalanceEvent.BalanceType.COLLATERAL,
        )
        self._create_mint_burn_transfer_balance_events(
            asset_token_address_pairs=stableDebtTokens,
            event_abis=all_events_abis,
            type=BalanceEvent.BalanceType.STABLE_DEBT,
        )
        self._create_mint_burn_transfer_balance_events(
            asset_token_address_pairs=variableDebtTokens,
            event_abis=all_events_abis,
            type=BalanceEvent.BalanceType.VARIABLE_DEBT,
        )

    def create_price_events(self):
        rows = clickhouse_client.select_rows(
            "AssetSourceUpdated",
        )

        all_abis = parse_json(file_path=PRICES_ABI_PATH)
        all_price_events_abis = get_all_event_abis(all_abis)

        price_events = PriceEvent.objects.all()
        postgres_price_events_cache = []
        for price_event in price_events.iterator():
            postgres_price_events_cache.append(
                self.get_price_events_cache_key(
                    price_event.asset, price_event.asset_source
                )
            )

        asset_source_token_metadata_rows = []

        for row in rows:
            clickhouse_asset = row[0]
            clickhouse_asset_source = row[1]

            clickhouse_price_event_cache = self.get_price_events_cache_key(
                clickhouse_asset, clickhouse_asset_source
            )
            if clickhouse_price_event_cache in postgres_price_events_cache:
                continue

            try:
                underlying_sources = get_underlying_sources(clickhouse_asset_source)
                asset_source_name, abi = RpcCacheStorage.get_contract_info(
                    clickhouse_asset_source
                )
                asset_source_token = Token(clickhouse_asset_source)
                decimals_places = asset_source_token.decimals()
                asset_source_token_metadata_rows.append(
                    [
                        clickhouse_asset_source,
                        decimals_places,
                        10**decimals_places,
                        int(datetime.now().timestamp() * 1_000_000),
                    ]
                )
            except UnsupportedAssetSourceError as e:
                logger.error(
                    f"Unsupported asset source for {clickhouse_asset_source}: {e}"
                )
                continue

            price_event = self.create_or_get_price_event(
                asset=clickhouse_asset,
                asset_source=clickhouse_asset_source,
                asset_source_name=asset_source_name,
                event_abis=all_price_events_abis,
                event_name="NewTransmission",
                contract_addresses=underlying_sources,
            )

        if asset_source_token_metadata_rows:
            clickhouse_client.insert_rows(
                "AssetSourceTokenMetadata", asset_source_token_metadata_rows
            )
            clickhouse_client.optimize_table("AssetSourceTokenMetadata")

    def activate_price_events(self):
        # Fetch all (asset, asset_source) pairs from ClickHouse result
        result = clickhouse_client.execute_query(
            "SELECT asset, source FROM aave_ethereum.LatestAssetSourceUpdated FINAL"
        )
        clickhouse_pairs = [(row[0], row[1]) for row in result.result_rows]

        with transaction.atomic():
            query = Q()
            PriceEvent.objects.all().update(is_active=False)
            if clickhouse_pairs:
                assetsources_q = Q()
                for asset, asset_source in clickhouse_pairs:
                    assetsources_q |= Q(asset=asset, asset_source=asset_source)
                query &= assetsources_q

                PriceEvent.objects.filter(query).update(is_active=True)
        logger.info(f"Activated {len(clickhouse_pairs)} price events")

    def run(self):
        """Execute the initialization task.

        Creates protocol events by reading configuration files and updating the database.
        Logs the start and completion of the task.
        """
        logger.info(f"Starting InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}")
        self.create_price_events()
        self.create_balance_events()
        logger.info(
            f"Completed InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}"
        )
        self.activate_price_events()
        UpdateTransmittersForPriceAggregatorsTask.delay()

    def create_or_get_price_event(
        self,
        asset: str,
        asset_source: str,
        asset_source_name: str,
        event_abis: Dict[str, Any],
        event_name: str,
        contract_addresses: List[str],
    ):
        """Create or get a PriceEvent instance for the given event configuration.

        Args:
            asset (str): The asset address
            asset_source (str): The asset source address
            asset_source_name (str): The name of the asset source
            event_abis (Dict[str, Any]): Dictionary of event ABIs from the protocol
            event_name (str): Name of the event to create/update
            contract_addresses (List[str]): List of contract addresses associated with the event

        Returns:
            PriceEvent: Created or existing PriceEvent instance

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

        event, is_created = PriceEvent.objects.update_or_create(
            asset=asset,
            asset_source=asset_source,
            name=event_name,
            defaults={
                "asset_source_name": asset_source_name,
                "topic_0": topic_0,
                "signature": signature,
                "abi": abi,
                "contract_addresses": contract_addresses,
                "is_enabled": True,
            },
        )

        if is_created:
            logger.info(
                f"Created PriceEvent instance: {event.name} for asset {asset} and source {asset_source} "
                f"on protocol {PROTOCOL_NAME} network {NETWORK_NAME}"
            )
        else:
            logger.info(
                f"Updated existing PriceEvent instance: {event.name} for asset {asset} and source {asset_source} "
                f"on protocol {PROTOCOL_NAME} network {NETWORK_NAME}"
            )

        return event

    def _create_or_get_balance_event(
        self,
        event_abis: Dict[str, Any],
        event_name: str,
        contract_addresses: List[str],
        asset: str,
        type: BalanceEvent.BalanceType,
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

        event, is_created = BalanceEvent.objects.update_or_create(
            contract_addresses=contract_addresses,
            asset=asset,
            name=event_name,
            type=type,
            defaults={
                "topic_0": topic_0,
                "signature": signature,
                "abi": abi,
                "is_enabled": True,
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


InitializePriceEvents = app.register_task(InitializePriceEvents())


class PriceEventSynchronizeTask(EventSynchronizeMixin, Task):
    event_model = PriceEvent
    clickhouse_client = clickhouse_client
    rpc_adapter = rpc_adapter
    network_name = NETWORK_NAME

    def run(self, event_ids: List[int]):
        """Default run method for child synchronize tasks."""
        active_event_ids = PriceEvent.objects.filter(
            is_active=True, id__in=event_ids
        ).values_list("id", flat=True)
        self.run_event_sync(active_event_ids)

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
        parsed_numerator_logs = []
        parsed_multiplier_logs = []
        updated_network_events = []

        # Ensure new transmission is the last event to sync. This ensures approximately
        # accurate prices when secondary fields like max asset cap are updated.

        for topic_0, event_logs in event_dicts.items():
            if not event_logs:
                continue

            filtered_network_events = network_events.filter(topic_0=topic_0)
            if not filtered_network_events.exists():
                continue

            grouped_event_logs = self.group_event_logs_by_address(event_logs)
            for address, event_logs_for_address in grouped_event_logs.items():
                network_events = filtered_network_events.filter(
                    contract_addresses__contains=address
                )
                for network_event in network_events.iterator():
                    parsed_numerator_logs.extend(
                        self.get_parsed_logs(
                            network_event, event_logs_for_address, get_numerator
                        )
                    )
                    parsed_multiplier_logs.extend(
                        self.get_parsed_logs(
                            network_event, event_logs_for_address, get_multiplier
                        )
                    )
                    network_event.logs_count += len(event_logs_for_address)
                    max_block_number = max(
                        event_log.blockNumber for event_log in event_logs_for_address
                    )
                    network_event.last_inserted_block = max_block_number
                    updated_network_events.append(network_event)

        self.bulk_insert_raw_price_events(
            table_name="EventRawNumerator", logs=parsed_numerator_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="TransactionRawNumerator", logs=parsed_numerator_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="EventRawMultiplier", logs=parsed_multiplier_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="TransactionRawMultiplier", logs=parsed_multiplier_logs
        )

        PriceEvent.objects.bulk_update(
            updated_network_events, ["logs_count", "last_inserted_block"]
        )

    def get_parsed_logs(
        self, network_event: PriceEvent, event_logs_for_address: List[Any], parser_func
    ):
        parsed_logs = []
        for event_log in event_logs_for_address:
            try:
                parsed_log = parser_func(
                    asset=network_event.asset,
                    asset_source=network_event.asset_source,
                    event=event_log,
                )
                parsed_logs.append(parsed_log)
            except web3.exceptions.BadFunctionCallOutput:
                continue
        return parsed_logs

    def bulk_insert_raw_price_events(self, table_name: str, logs: List[List[Any]]):
        if not logs:
            logger.info(f"No logs found for {table_name}. Skipping bulk insert.")
            return

        for i in range(3):
            try:
                self.clickhouse_client.insert_rows(table_name, logs)
                break
            except Exception as e:
                logger.error(f"Error inserting rows: {e}")

        for i in range(3):
            try:
                self.clickhouse_client.optimize_table(table_name)
                break
            except Exception as e:
                logger.error(f"Error optimizing table: {e}")

    def post_handle_hook(
        self, network_events: List[Any], start_block: int, end_block: int
    ):
        UpdateTransmittersForPriceAggregatorsTask.delay()


PriceEventSynchronizeTask = app.register_task(PriceEventSynchronizeTask())


class UpdateTransmittersForPriceAggregatorsTask(Task):
    def run(self):
        """
        Retrieves transmitters for price aggregators and updates the PriceEvent model.
        """

        self.set_transmitters()
        self.set_authorized_senders()
        self.cache_transmitters_for_websockets()
        self.cache_authorized_senders_for_websockets()
        self.cache_asset_sources_for_websockets()

    def set_transmitters(self):
        price_events = PriceEvent.objects.filter(is_active=True).exclude(
            asset_source="0xd110cac5d8682a3b045d5524a9903e031d70fccd"
        )
        # Exclude retrieving a transmitter for the GhoOracle
        # GHOOracle uses a hardcoded value and does not change ever
        updated_price_events = []
        for price_event in price_events.iterator():
            contract_addresses = price_event.contract_addresses
            if contract_addresses:
                for contract_address in contract_addresses:
                    transmitters = RpcCacheStorage.get_cached_asset_source_function(
                        asset_source=contract_address,
                        function_name="getTransmitters",
                        ttl=CACHE_TTL_4_HOURS,
                    )

                    if transmitters:
                        if isinstance(transmitters, list):
                            transmitters = list(set(transmitters))
                        else:
                            transmitters = list(
                                set([t for t in dict(transmitters).values()])
                            )

                        price_event.transmitters = [
                            transmitter.lower() for transmitter in transmitters
                        ]
                        updated_price_events.append(price_event)
        PriceEvent.objects.bulk_update(updated_price_events, ["transmitters"])

    def set_authorized_senders(self):
        price_events = PriceEvent.objects.filter(is_active=True).exclude(
            asset_source="0xd110cac5d8682a3b045d5524a9903e031d70fccd"
        )
        updated_price_events = []
        for price_event in price_events.iterator():
            all_authorized_senders = []
            for transmitter in price_event.transmitters:
                authorized_senders = RpcCacheStorage.get_cached_asset_source_function(
                    asset_source=transmitter,
                    function_name="getAuthorizedSenders",
                    ttl=CACHE_TTL_4_HOURS,
                )

                if authorized_senders:
                    if isinstance(authorized_senders, list):
                        authorized_senders = [a for a in list(set(authorized_senders))]
                    else:
                        authorized_senders = list(
                            set([a for a in dict(authorized_senders).values()])
                        )
                    all_authorized_senders.extend(authorized_senders)
            all_authorized_senders = [a.lower() for a in all_authorized_senders]
            price_event.authorized_senders = list(set(all_authorized_senders))
            updated_price_events.append(price_event)
        PriceEvent.objects.bulk_update(updated_price_events, ["authorized_senders"])

    def cache_transmitters_for_websockets(self):
        # Get only active asset sources from utils
        active_price_events = get_latest_asset_sources()
        transmitters_qs = active_price_events.values_list("transmitters", flat=True)
        transmitters = []
        for transmitters_array in transmitters_qs:
            if transmitters_array:
                transmitters.extend(transmitters_array)
        transmitters = list(set(transmitters))
        cache.set("transmitters_for_websockets", transmitters)

    def cache_authorized_senders_for_websockets(self):
        active_price_events = get_latest_asset_sources()
        authorized_senders_qs = active_price_events.values_list(
            "authorized_senders", flat=True
        )
        authorized_senders = []
        for authorized_senders_array in authorized_senders_qs:
            if authorized_senders_array:
                authorized_senders.extend(authorized_senders_array)
        authorized_senders = list(set(authorized_senders))
        cache.set("authorized_senders_for_websockets", authorized_senders)

    def cache_asset_sources_for_websockets(self):
        # Get only active asset sources from utils
        active_price_events = get_latest_asset_sources()
        # Create a dictionary to group by contract_address
        contract_address_to_asset_sources = {}

        for event in active_price_events:
            contract_addresses = event.contract_addresses
            if contract_addresses:
                for contract_address in contract_addresses:
                    if contract_address not in contract_address_to_asset_sources:
                        contract_address_to_asset_sources[contract_address] = []

                    contract_address_to_asset_sources[contract_address].append(
                        [event.asset, event.asset_source]
                    )

        # Cache asset_sources for each contract address
        for (
            contract_address,
            asset_sources_list,
        ) in contract_address_to_asset_sources.items():
            cache_key = f"underlying_asset_source_{contract_address}"
            cache.set(cache_key, asset_sources_list)


UpdateTransmittersForPriceAggregatorsTask = app.register_task(
    UpdateTransmittersForPriceAggregatorsTask()
)


class PriceEventDynamicSynchronizeTask(ParentSynchronizeTaskMixin, Task):
    event_model = PriceEvent
    child_task = PriceEventSynchronizeTask


PriceEventDynamicSynchronizeTask = app.register_task(PriceEventDynamicSynchronizeTask())


class BasePriceMixin:
    def get_parsed_logs(
        self, network_event: PriceEvent, event_logs_for_address: List[Any], parser_func
    ):
        parsed_logs = []
        for event_log in event_logs_for_address:
            parsed_logs.append(
                parser_func(
                    asset=network_event.asset,
                    asset_source=network_event.asset_source,
                    event=event_log,
                )
            )
        return parsed_logs

    def bulk_insert_raw_price_events(self, table_name: str, logs: List[List[Any]]):
        for i in range(3):
            try:
                clickhouse_client.insert_rows(table_name, logs)
                break
            except Exception as e:
                logger.error(f"Error inserting rows: {e}")

        for i in range(3):
            try:
                clickhouse_client.optimize_table(table_name)
                break
            except Exception as e:
                logger.error(f"Error optimizing table: {e}")


class PriceEventStaticSynchronizeTask(BasePriceMixin, Task):
    def run(self):
        # Get only active asset sources from utils
        network_events = get_latest_asset_sources()

        parsed_denominator_logs = []
        parsed_max_cap_logs = []

        for network_event in network_events.iterator():
            event = AttributeDict({"blockNumber": rpc_adapter.cached_block_height})
            parsed_denominator_logs.extend(
                self.get_parsed_logs(network_event, [event], get_denominator)
            )
            parsed_max_cap_logs.extend(
                self.get_parsed_logs(network_event, [event], get_max_cap)
            )

        self.bulk_insert_raw_price_events(
            table_name="EventRawDenominator", logs=parsed_denominator_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="EventRawMaxCap", logs=parsed_max_cap_logs
        )


PriceEventStaticSynchronizeTask = app.register_task(PriceEventStaticSynchronizeTask())


class PriceTransactionDynamicSynchronizeTask(BasePriceMixin, Task):
    def run(self):
        # Get only active asset sources from utils
        network_events = get_latest_asset_sources()

        parsed_multiplier_logs = []

        for network_event in network_events.iterator():
            event = AttributeDict({"blockNumber": rpc_adapter.cached_block_height})
            parsed_multiplier_logs.extend(
                self.get_parsed_logs(network_event, [event], get_multiplier)
            )

        self.bulk_insert_raw_price_events(
            table_name="TransactionRawMultiplier", logs=parsed_multiplier_logs
        )

        self.bulk_insert_raw_price_events(
            table_name="EventRawMultiplier", logs=parsed_multiplier_logs
        )
        # Refresh MultiplierStatistics view and dictionary after updating multiplier data
        self.refresh_multiplier_statistics()

    def refresh_multiplier_statistics(self):
        """Refresh the MultiplierStatistics view and reload the dictionary."""
        try:
            # The view is refreshed automatically when queried since it's a regular view
            # We just need to reload the dictionary to pick up new data
            logger.info("Reloading MultiplierStatsDict dictionary")
            clickhouse_client.execute_query(
                "SYSTEM RELOAD DICTIONARY aave_ethereum.MultiplierStatsDict"
            )
            logger.info("Successfully reloaded MultiplierStatsDict dictionary")
        except Exception as e:
            logger.error(f"Error refreshing multiplier statistics: {e}")


PriceTransactionDynamicSynchronizeTask = app.register_task(
    PriceTransactionDynamicSynchronizeTask()
)


class InsertTransactionNumeratorTask(BasePriceMixin, Task):
    def run(self, parsed_multiplier_logs: List[Any]):
        self.bulk_insert_raw_price_events(
            table_name="TransactionRawNumerator", logs=parsed_multiplier_logs
        )


InsertTransactionNumeratorTask = app.register_task(InsertTransactionNumeratorTask())


class VerifyHistoricalPriceTask(Task):
    def _compare_price_with_rpc(
        self,
        price,
        rpc_price,
        price_type,
        asset,
        asset_source,
        name,
        threshold,
        mismatch_counts,
        verification_records,
    ):
        """Compare a price with RPC price and track mismatches."""
        if price is None:
            return True

        # Handle zero rpc_price to avoid division by zero
        if rpc_price == 0:
            return True  # Skip comparison for zero prices

        percent_diff = (price - rpc_price) / rpc_price

        # Store verification record
        verification_records.append(
            [
                asset,
                asset_source,
                name,
                price_type,
                int(datetime.now().timestamp() * 1_000_000),  # Convert to microseconds
                (percent_diff),
            ]
        )

        if rpc_price != 0 and abs(
            Decimal(str(price)) - Decimal(str(rpc_price))
        ) / Decimal(str(rpc_price)) > Decimal(str(threshold)):
            mismatch_counts[f"{price_type}_vs_rpc"] += 1
            return False
        return True

    def run(self):
        latest_price_events = get_latest_asset_sources()
        num_verified = 0
        num_different = 0
        delta_threshold = 0.00001
        mismatch_counts = {
            "historical_event_vs_rpc": 0,
            "historical_transaction_vs_rpc": 0,
            "predicted_transaction_vs_rpc": 0,
        }
        verification_records = []

        for price_event in latest_price_events.iterator():
            asset_source_interface = PriceOracleInterface(
                asset=price_event.asset, asset_source=price_event.asset_source
            )

            try:
                historical_event = asset_source_interface.historical_price_from_event
                historical_transaction = (
                    asset_source_interface.historical_price_from_transaction
                )
                predicted_transaction = (
                    asset_source_interface.predicted_price_from_transaction
                )
                latest_price_from_rpc = asset_source_interface.latest_price_from_rpc

            except Exception as e:
                logger.error(
                    f"Error getting prices for {price_event.asset} {price_event.asset_source}: {e}"
                )
                continue

            # Compare all three price types using the same function
            matches = [
                self._compare_price_with_rpc(
                    price=historical_event,
                    rpc_price=latest_price_from_rpc,
                    price_type="historical_event",
                    asset=price_event.asset,
                    asset_source=price_event.asset_source,
                    name=price_event.asset_source_name,
                    threshold=delta_threshold,
                    mismatch_counts=mismatch_counts,
                    verification_records=verification_records,
                ),
                self._compare_price_with_rpc(
                    price=historical_transaction,
                    rpc_price=latest_price_from_rpc,
                    price_type="historical_transaction",
                    asset=price_event.asset,
                    asset_source=price_event.asset_source,
                    name=price_event.asset_source_name,
                    threshold=delta_threshold,
                    mismatch_counts=mismatch_counts,
                    verification_records=verification_records,
                ),
                self._compare_price_with_rpc(
                    price=predicted_transaction,
                    rpc_price=latest_price_from_rpc,
                    price_type="predicted_transaction",
                    asset=price_event.asset,
                    asset_source=price_event.asset_source,
                    name=price_event.asset_source_name,
                    threshold=delta_threshold,
                    mismatch_counts=mismatch_counts,
                    verification_records=verification_records,
                ),
            ]

            if all(matches):
                num_verified += 1
            else:
                num_different += 1

        # Insert verification records into ClickHouse
        if verification_records:
            try:
                clickhouse_client.insert_rows(
                    "PriceVerificationRecords", verification_records
                )
                logger.info(
                    f"Inserted {len(verification_records)} verification records"
                )
            except Exception as e:
                logger.error(f"Error inserting verification records: {e}")

        # Insert mismatch counts into ClickHouse table
        mismatch_record = [
            [
                int(
                    datetime.now().timestamp() * 1_000_000
                ),  # insert_timestamp in microseconds
                mismatch_counts["historical_event_vs_rpc"],
                mismatch_counts["historical_transaction_vs_rpc"],
                mismatch_counts["predicted_transaction_vs_rpc"],
                num_verified,
                num_different,
            ]
        ]

        try:
            clickhouse_client.insert_rows("PriceMismatchCounts", mismatch_record)
            logger.info(f"Inserted mismatch counts: {mismatch_counts}")
        except Exception as e:
            logger.error(f"Error inserting mismatch counts: {e}")

        logger.info(f"Verified {num_verified} assets, {num_different} different")
        logger.info(f"Mismatch breakdown: {mismatch_counts}")


VerifyHistoricalPriceTask = app.register_task(VerifyHistoricalPriceTask())
