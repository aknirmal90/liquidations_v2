import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List

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
                        10 ** decimals_places,
                        int(datetime.now().timestamp()),
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
        topic_0s = list(event_dicts.keys())
        NEW_TRANSMISSION_TOPIC_0 = (
            "0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a"
        )
        if NEW_TRANSMISSION_TOPIC_0 in topic_0s:
            topic_0s.remove(NEW_TRANSMISSION_TOPIC_0)
            topic_0s.insert(len(topic_0s), NEW_TRANSMISSION_TOPIC_0)

        # Ensure new transmission is the last event to sync. This ensures approximately
        # accurate prices when secondary fields like max asset cap are updated.

        for topic_0 in topic_0s:
            event_logs = event_dicts[topic_0]
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
                    self.process_network_event(network_event, event_logs_for_address)

    def process_network_event(self, network_event: PriceEvent, event_logs: List[Any]):
        contract_interface = network_event.contract_interface
        log_fields = [
            col_name for col_name, _ in network_event._get_clickhouse_log_columns()
        ]

        parsed_event_logs = []
        timestamps = self.get_timestamps_for_events(event_logs)

        for event_log in event_logs:
            log_values = [
                getattr(event_log, field)
                for field in log_fields
                if field != "blockTimestamp"
            ]
            log_values.append(timestamps[event_log.blockNumber])
            processed_event_log = contract_interface.process_event(event_log)

            parsed_event_log = processed_event_log + log_values
            parsed_event_logs.append(parsed_event_log)

        for i in range(3):
            try:
                self.clickhouse_client.insert_rows("RawPriceEvent", parsed_event_logs)
                break
            except Exception as e:
                logger.error(f"Error inserting rows: {e}")
                time.sleep(1)

        for i in range(3):
            try:
                self.clickhouse_client.optimize_table("RawPriceEvent")
                break
            except Exception as e:
                logger.error(f"Error optimizing table: {e}")
                time.sleep(1)

        network_event.logs_count += len(event_logs)
        network_event.save()
        logger.info(f"Number of records inserted: {len(event_logs)}")


PriceEventSynchronizeTask = app.register_task(PriceEventSynchronizeTask())


class PriceEventParentSynchronizeTask(ParentSynchronizeTaskMixin, Task):
    event_model = PriceEvent
    child_task = PriceEventSynchronizeTask


PriceEventParentSynchronizeTask = app.register_task(PriceEventParentSynchronizeTask())
