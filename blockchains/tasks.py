import logging
import os
from typing import Any, Dict, List

from celery import Task
from django.conf import settings
from django.core.cache import cache
from eth_utils import get_all_event_abis

from blockchains.models import Event
from liquidations_v2.celery_app import app
from oracles.models import PriceEvent
from utils.clickhouse.client import clickhouse_client
from utils.constants import (
    NETWORK_BLOCK_TIME,
    NETWORK_NAME,
    PROTOCOL_ABI_PATH,
    PROTOCOL_CONFIG_PATH,
    PROTOCOL_NAME,
)
from utils.encoding import get_signature, get_topic_0
from utils.files import parse_json, parse_yaml
from utils.rpc import rpc_adapter
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
            (network_name, latest_block_number, latest_block_timestamp, network_time_for_new_block)
            VALUES ('{NETWORK_NAME}', {block_number}, {block_timestamp * 1_000_000}, {NETWORK_BLOCK_TIME})
            """

            clickhouse_client.execute_query(query)
            clickhouse_client.optimize_table("NetworkBlockInfo")

            logger.info(
                f"Updated NetworkBlockInfo for {NETWORK_NAME}: block {block_number}, timestamp {block_timestamp}"
            )

        except Exception as e:
            logger.error(f"Failed to update NetworkBlockInfo: {e}")
            raise


UpdateNetworkBlockInfoTask = app.register_task(UpdateNetworkBlockInfoTask())
