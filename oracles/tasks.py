import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List

import web3
from celery import Task
from django.conf import settings
from eth_utils import get_all_event_abis

from liquidations_v2.celery_app import app
from oracles.contracts.service import (
    UnsupportedAssetSourceError,
    get_contract_interface,
)
from oracles.models import PriceEvent
from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_NAME, PRICES_ABI_PATH, PROTOCOL_NAME
from utils.encoding import get_signature, get_topic_0
from utils.files import parse_json
from utils.rpc import rpc_adapter
from utils.tasks import EventSynchronizeMixin, ParentSynchronizeTaskMixin
from utils.tokens import Token

logger = logging.getLogger(__name__)


class InitializePriceEvents(Task):
    def get_price_events_cache_key(self, asset: str, asset_source: str) -> str:
        return f"{PROTOCOL_NAME}-{NETWORK_NAME}-{asset}-{asset_source}"

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
                asset_source_interface = get_contract_interface(
                    asset=clickhouse_asset, asset_source=clickhouse_asset_source
                )
                asset_source_name = asset_source_interface.name
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

            # Get event names from the ABI
            for event_name in asset_source_interface.events:
                # Create or get the price event with asset-specific information
                price_event = self.create_or_get_price_event(
                    asset=clickhouse_asset,
                    asset_source=clickhouse_asset_source,
                    asset_source_name=asset_source_name,
                    event_abis=all_price_events_abis,
                    event_name=event_name,
                    contract_addresses=asset_source_interface.get_underlying_sources_to_monitor(),
                    method_ids=asset_source_interface.method_ids,
                )

        if asset_source_token_metadata_rows:
            clickhouse_client.insert_rows(
                "AssetSourceTokenMetadata", asset_source_token_metadata_rows
            )
            clickhouse_client.optimize_table("AssetSourceTokenMetadata")

    def run(self):
        """Execute the initialization task.

        Creates protocol events by reading configuration files and updating the database.
        Logs the start and completion of the task.
        """
        logger.info(f"Starting InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}")
        self.create_price_events()
        self.create_materialized_views()
        logger.info(
            f"Completed InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}"
        )
        self.optimize_tables()

    def optimize_tables(self):
        """Optimize tables in Clickhouse."""
        clickhouse_client.optimize_table("EventRawNumerator")
        clickhouse_client.optimize_table("EventRawDenominator")
        clickhouse_client.optimize_table("EventRawMultiplier")
        clickhouse_client.optimize_table("EventRawMaxCap")
        clickhouse_client.optimize_table("TransactionRawNumerator")
        clickhouse_client.optimize_table("TransactionRawDenominator")
        clickhouse_client.optimize_table("TransactionRawMultiplier")

    def create_materialized_views(self):
        """Create materialized views in Clickhouse."""
        MATERIALIZED_VIEWS_PATH = os.path.join(
            os.path.dirname(settings.BASE_DIR), "oracles", "mv_queries"
        )
        files = os.listdir(MATERIALIZED_VIEWS_PATH)
        files.sort()

        for filename in files:
            if not filename.endswith(".sql"):
                continue

            with open(os.path.join(MATERIALIZED_VIEWS_PATH, filename), "r") as file:
                query = file.read()
                logger.info(f"Executing query: {filename}")
                clickhouse_client.execute_query(query)

    def create_or_get_price_event(
        self,
        asset: str,
        asset_source: str,
        asset_source_name: str,
        event_abis: Dict[str, Any],
        event_name: str,
        contract_addresses: List[str],
        method_ids: List[str],
    ):
        """Create or get a PriceEvent instance for the given event configuration.

        Args:
            asset (str): The asset address
            asset_source (str): The asset source address
            asset_source_name (str): The name of the asset source
            event_abis (Dict[str, Any]): Dictionary of event ABIs from the protocol
            event_name (str): Name of the event to create/update
            contract_addresses (List[str]): List of contract addresses associated with the event
            method_ids (List[str]): List of method IDs for the asset source

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
                "method_ids": method_ids,
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


InitializePriceEvents = app.register_task(InitializePriceEvents())


class PriceEventSynchronizeTask(EventSynchronizeMixin, Task):
    event_model = PriceEvent
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
        parsed_numerator_logs = []
        parsed_denominator_logs = []
        parsed_multiplier_logs = []
        parsed_max_cap_logs = []
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
                    (
                        new_parsed_numerator_logs,
                        new_parsed_denominator_logs,
                        new_parsed_multiplier_logs,
                        new_parsed_max_cap_logs,
                    ) = self.process_network_event(
                        network_event, event_logs_for_address
                    )

                    parsed_numerator_logs.extend(new_parsed_numerator_logs)
                    parsed_denominator_logs.extend(new_parsed_denominator_logs)
                    parsed_multiplier_logs.extend(new_parsed_multiplier_logs)
                    parsed_max_cap_logs.extend(new_parsed_max_cap_logs)

                    network_event.logs_count += len(event_logs_for_address)
                    updated_network_events.append(network_event)

        self.bulk_insert_raw_price_events(
            table_name="EventRawNumerator", logs=parsed_numerator_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="EventRawDenominator", logs=parsed_denominator_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="EventRawMultiplier", logs=parsed_multiplier_logs
        )
        self.bulk_insert_raw_price_events(
            table_name="EventRawMaxCap", logs=parsed_max_cap_logs
        )

        PriceEvent.objects.bulk_update(updated_network_events, ["logs_count"])

    def process_network_event(self, network_event: PriceEvent, event_logs: List[Any]):
        contract_interface = network_event.contract_interface
        parsed_numerator_logs = []
        parsed_denominator_logs = []
        parsed_multiplier_logs = []
        parsed_max_cap_logs = []

        for event_log in event_logs:
            try:
                parsed_event_log = contract_interface.get_price_components(
                    event=event_log
                )
            except web3.exceptions.BadFunctionCallOutput:
                continue

            parsed_numerator_logs.append(
                self._get_log_properties(parsed_event_log, "numerator")
            )
            parsed_denominator_logs.append(
                self._get_log_properties(parsed_event_log, "denominator")
            )
            parsed_multiplier_logs.append(
                self._get_log_properties(parsed_event_log, "multiplier")
            )
            parsed_max_cap_logs.append(
                self._get_log_properties(parsed_event_log, "max_cap")
            )

        return (
            parsed_numerator_logs,
            parsed_denominator_logs,
            parsed_multiplier_logs,
            parsed_max_cap_logs,
        )

    def _get_log_properties(self, event_log, prop: str):
        super_properties = [
            "asset",
            "asset_source",
            "name",
            "timestamp",
        ]
        properties = super_properties + [prop]
        return [event_log[property] for property in properties]

    def bulk_insert_raw_price_events(self, table_name: str, logs: List[List[Any]]):
        for i in range(3):
            try:
                self.clickhouse_client.insert_rows(table_name, logs)
                break
            except Exception as e:
                logger.error(f"Error inserting rows: {e}")
                time.sleep(1)

        for i in range(3):
            try:
                self.clickhouse_client.optimize_table(table_name)
                break
            except Exception as e:
                logger.error(f"Error optimizing table: {e}")
                time.sleep(1)


PriceEventSynchronizeTask = app.register_task(PriceEventSynchronizeTask())


class PriceEventParentSynchronizeTask(ParentSynchronizeTaskMixin, Task):
    event_model = PriceEvent
    child_task = PriceEventSynchronizeTask


PriceEventParentSynchronizeTask = app.register_task(PriceEventParentSynchronizeTask())


class VerifyHistoricalPriceTask(Task):
    def run(self):
        all_assets = clickhouse_client.execute_query(
            "SELECT asset, source AS asset_source FROM aave_ethereum.LatestAssetSourceUpdated FINAL"
        )
        num_verified = 0
        num_different = 0
        delta_threshold = 0.01

        for asset, asset_source in all_assets.result_rows:
            asset_source_interface = get_contract_interface(
                asset=asset, asset_source=asset_source
            )
            try:
                historical_price_from_event = (
                    asset_source_interface.historical_price_from_event
                )
            except Exception as e:
                logger.error(
                    f"Error getting historical price from event for {asset} {asset_source}: {e}"
                )
                continue

            latest_price_from_rpc = asset_source_interface.latest_price_from_rpc
            if (
                abs(historical_price_from_event - latest_price_from_rpc)
                / latest_price_from_rpc
                > delta_threshold
            ):
                num_different += 1
                logger.error(
                    f"Historical price from clickhouse and rpc are different for {asset} {asset_source}"
                )
                logger.error(
                    f"Historical Event Price: {historical_price_from_event}, RPC: {latest_price_from_rpc} for {asset_source_interface.name} {asset_source}"
                )
            else:
                num_verified += 1

        logger.info(f"Verified {num_verified} assets, {num_different} different")


VerifyHistoricalPriceTask = app.register_task(VerifyHistoricalPriceTask())
