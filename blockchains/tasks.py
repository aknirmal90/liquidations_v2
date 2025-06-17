import logging

# from collections import defaultdict
# from datetime import datetime
from typing import Any, Dict, List

# import pytz
from celery import Task
from django.core.cache import cache
from eth_utils import get_all_event_abis

# from aave.models import Asset
# from aave.tasks import ResetAssetsTask
from blockchains.models import Event
from liquidations_v2.celery_app import app
from utils.clickhouse.client import clickhouse_client
from utils.constants import (
    NETWORK_NAME,
    PROTOCOL_ABI_PATH,
    PROTOCOL_CONFIG_PATH,
    PROTOCOL_NAME,
)

# from utils.constants import ACCEPTABLE_EVENT_BLOCK_LAG_DELAY
from utils.encoding import get_signature, get_topic_0
from utils.files import parse_json, parse_yaml

# from django.db.models import F
# from web3 import Web3
# from web3._utils.events import get_event_data
# from web3.exceptions import Web3RPCError


# from utils.rpc import get_evm_block_timestamps

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
        logger.info(
            f"Completed InitializeAppTask for {PROTOCOL_NAME} on {NETWORK_NAME}"
        )

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

        # Clear cache
        cache.clear()
        logger.info("Redis Cache cleared")

        logger.info("App reset complete")


ResetAppTask = app.register_task(ResetAppTask())


# class BaseSynchronizeTask(Task):
#     """
#     Syncs event logs from contracts, signatures, and block ranges for a list of event IDs.
#     """
#     abstract = True
#     expires = 1 * 60  # 1 minute in seconds
#     time_limit = 1 * 60  # 1 minute in seconds

#     def get_queryset(self, event_ids: List[int]):
#         raise NotImplementedError

#     def get_aave_pricesources(self, network: Network):
#         return Asset.objects.filter(network=network).values_list('pricesource', flat=True)

#     def get_aave_atokens(self, network: Network):
#         return Asset.objects.filter(network=network).values_list('atoken_address', flat=True)

#     def get_aave_variable_debt_tokens(self, network: Network):
#         return Asset.objects.filter(network=network).values_list('variable_debt_token_address', flat=True)

#     def sync_events_for_network(self, network: Network, network_events: List[Event]):
#         rpc_adapter = network.rpc_adapter
#         global_to_block = rpc_adapter.block_height
#         global_from_block = min(event.last_synced_block for event in network_events)
#         if global_from_block != 0:
#             global_from_block += 1
#             # Data has been synced until here, so we start from the next block

#         EVENTS_ARRAY_THRESHOLD_SIZE = 5000

#         if global_from_block >= global_to_block:
#             logger.debug(f"{network.name} has no new blocks. Nothing to sync.")
#             return

#         iter_from_block = global_from_block
#         iter_delta = min(global_to_block - global_from_block, rpc_adapter.max_blockrange_size_for_events)
#         iter_to_block = global_from_block + iter_delta
#         contract_addresses = list(set(
#             Web3.to_checksum_address(address)
#             for event in network_events
#             for address in event.contract_addresses
#         ))
#         contract_addresses.extend([
#             Web3.to_checksum_address(address)
#             for address in self.get_aave_pricesources(network)
#             if address
#         ])
#         contract_addresses.extend([
#             Web3.to_checksum_address(address)
#             for address in self.get_aave_atokens(network)
#             if address
#         ])
#         contract_addresses.extend([
#             Web3.to_checksum_address(address)
#             for address in self.get_aave_variable_debt_tokens(network)
#             if address
#         ])

#         while True:
#             try:
#                 if (iter_from_block - iter_to_block) >= 0:
#                     break

#                 logger.info(
#                     f"Event Extraction for network {network.name} "
#                     f"from {iter_from_block} to {iter_to_block}"
#                 )

#                 topics = [event.topic_0 for event in network_events]
#                 event_abis = {event.topic_0: event.abi for event in network_events}

#                 raw_event_dicts = rpc_adapter.extract_raw_event_data(
#                     topics=topics,
#                     contract_addresses=contract_addresses,
#                     start_block=iter_from_block,
#                     end_block=iter_to_block,
#                 )

#                 event_dicts = self.process_raw_event_dicts(raw_event_dicts=raw_event_dicts, event_abis=event_abis)
#                 # cleaned_event_dicts = self.clean_event_logs(network_events, event_dicts)
#                 self.handle_event_logs(network_events=network_events, cleaned_event_dicts=event_dicts)

#                 if iter_to_block >= global_to_block:
#                     self.update_last_synced_block(network_events, global_to_block)
#                     logger.info(
#                         f"Event Extraction for network {network.name} "
#                         f"has completed from {global_from_block} to {global_to_block}"
#                     )
#                     break
#                 else:
#                     iter_delta = min(iter_to_block - iter_from_block, rpc_adapter.max_blockrange_size_for_events)
#                     self.update_last_synced_block(network_events, iter_to_block)
#                     iter_from_block = iter_to_block + 1

#                     if len(event_dicts) >= EVENTS_ARRAY_THRESHOLD_SIZE:
#                         iter_to_block += int(iter_delta / 2)
#                     else:
#                         iter_to_block += min(iter_delta * 2, rpc_adapter.max_blockrange_size_for_events)

#                     if iter_to_block >= global_to_block:
#                         iter_to_block = global_to_block

#             except Web3RPCError as e:
#                 if e.rpc_response['error']['code'] == -32005:
#                     logger.error(e)
#                     iter_delta = iter_delta // 2
#                     iter_to_block = iter_from_block + iter_delta

#                     if iter_to_block >= global_to_block:
#                         iter_to_block = global_to_block
#                 else:
#                     raise e

#     def process_raw_event_dicts(self, raw_event_dicts, event_abis):
#         event_dicts = {}
#         counter = 0
#         codec = Web3().codec
#         if not raw_event_dicts:
#             return {}

#         for log in raw_event_dicts:
#             counter += 1
#             topic0 = f"0x{log['topics'][0].hex()}"
#             event_abi = event_abis.get(topic0)

#             if event_abi:
#                 event_data = decode_any(get_event_data(codec, event_abi, log))
#                 if topic0 not in event_dicts:
#                     event_dicts[topic0] = []
#                 event_dicts[topic0].append(event_data)
#         return event_dicts

#     def clean_event_logs(self, network_events, event_dicts):
#         """
#         Filters out events accidentally sent in or if they need to be
#         filtered by contract address
#         """
#         cleaned_event_dicts = {}
#         unique_log_comments = set()
#         events = {event: event.topic_0 for event in network_events}

#         for event, topic0 in events.items():
#             cleaned_event_logs = []
#             event_logs = event_dicts.get(topic0, {})

#             contract_addresses = event.contract_addresses

#             for event_log in event_logs:
#                 if not contract_addresses:
#                     cleaned_event_logs.append(event_log)
#                 else:
#                     if event_log['address'].lower() in contract_addresses:
#                         cleaned_event_logs.append(event_log)
#                     else:
#                         comment = f"Filtered out log with address: {event_log['address']} for event {event.id}"
#                         unique_log_comments.add(comment)

#             cleaned_event_dicts[event.id] = cleaned_event_logs

#             for comment in unique_log_comments:
#                 logger.info(comment)

#         return cleaned_event_dicts

#     def handle_event_logs(self, network_events: List[Event], cleaned_event_dicts: List[Dict]):
#         """
#         Saves a list of events to its respective model class
#         """
#         for network_event in network_events:
#             adapter = network_event.get_adapter()
#             func_name = f"parse_{network_event.name}"
#             func = getattr(adapter, func_name)
#             event_logs = cleaned_event_dicts.get(network_event.topic_0, [])
#             if event_logs:
#                 func(network_event, event_logs)
#                 logger.info(f"Number of records inserted: {len(event_logs)}")

#     def update_last_synced_block(self, events: List[Event], block: int):
#         Event.objects.filter(id__in=[event.id for event in events]).update(
#             last_synced_block=block, updated_at=datetime.now(pytz.utc)
#         )


# class BaseEventSynchronizeTask(BaseSynchronizeTask):
#     abstract = True

#     def run(self):
#         events = self.get_queryset()
#         events_by_network = group_events_by_network(events)

#         for network, network_events in events_by_network.items():
#             try:
#                 self.sync_events_for_network(network, network_events)
#             except Exception as e:
#                 names = [event.name for event in network_events]
#                 logger.error(
#                     f"Failed to sync events for network {network.name}: {str(e)}. Events: {names}",
#                     exc_info=True
#                 )


# class StreamingSynchronizeForEventTask(BaseEventSynchronizeTask):
#     def get_queryset(self):
#         return Event.objects.filter(
#             last_synced_block__gt=F("network__latest_block") - ACCEPTABLE_EVENT_BLOCK_LAG_DELAY,
#             is_enabled=True
#         )


# StreamingSynchronizeForEventTask = app.register_task(
#     StreamingSynchronizeForEventTask()
# )


# class BackfillSynchronizeForEventTask(BaseEventSynchronizeTask):
#     expires = 6 * 60 * 60  # 6 hours in seconds
#     time_limit = 6 * 60 * 60  # 6 hours in seconds

#     def get_queryset(self):
#         return Event.objects.filter(
#             last_synced_block__lte=F("network__latest_block") - ACCEPTABLE_EVENT_BLOCK_LAG_DELAY,
#             is_enabled=True
#         )


# BackfillSynchronizeForEventTask = app.register_task(
#     BackfillSynchronizeForEventTask()
# )


# class UpdateMetadataCacheTask(Task):
#     def run(self):
#         logger.info("Starting metadata cache update...")
#         logger.info("Updating asset cache...")
#         for asset in Asset.objects.all():
#             logger.info(f"Caching asset {asset.asset} on {asset.network.name}")
#             Asset.get_by_address(network_name=asset.network.name, token_address=asset.asset)
#             Asset.get_by_id(id=asset.id)

#         logger.info("Completed metadata cache update")


# UpdateMetadataCacheTask = app.register_task(UpdateMetadataCacheTask())
