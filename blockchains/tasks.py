import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List

import pytz
from celery import Task
from django.core.cache import cache
from django.db.models import F
from web3 import Web3
from web3._utils.events import get_event_data
from web3.exceptions import Web3RPCError

from aave.models import Asset
from aave.tasks import ResetAssetsTask
from blockchains.models import Event, Network, Protocol
from liquidations_v2.celery_app import app
from utils.constants import ACCEPTABLE_EVENT_BLOCK_LAG_DELAY
from utils.encoding import decode_any, get_signature, get_topic_0
from utils.files import parse_yaml

logger = logging.getLogger(__name__)


class InitializeAppTask(Task):
    def run(self):
        logger.info("Starting InitializeAppTask")
        protocols = parse_yaml("protocols.yaml")
        networks = parse_yaml("networks.yaml")

        self.create_protocol_instances(protocols)
        self.create_network_instances(networks)

        protocol_instances = Protocol.objects.all()
        for protocol in protocol_instances:
            self.create_protocol_events(protocol)

        logger.info("Completed InitializeAppTask")

    def create_protocol_instances(self, protocols):
        for protocol_data in protocols:
            Protocol.objects.update_or_create(name=protocol_data['name'], defaults=protocol_data)
            logger.info(f"Created Protocol instance: {protocol_data['name']}")

    def create_network_instances(self, networks):
        for network_data in networks:
            Network.objects.update_or_create(name=network_data['name'], defaults=network_data)
            logger.info(f"Created Network instance: {network_data['name']}")

    def create_protocol_events(self, protocol: Protocol):
        config = protocol.config
        network_configs = config["networks"]
        event_configs = config["events"]

        for network_config in network_configs:
            network = Network.objects.get(name=network_config["network"])
            contract_addresses = network_config.get("contract_addresses", [])
            contract_addresses = [address.lower() for address in contract_addresses]
            for event_config in event_configs:
                self.create_or_get_event(
                    protocol=protocol,
                    network=network,
                    event_config=event_config,
                    contract_addresses=contract_addresses
                )

    def create_or_get_event(
        self,
        protocol: Protocol,
        network: Network,
        event_config: Dict[str, Any],
        contract_addresses: List[str]
    ):
        """Create or get an Event instance for the given protocol, network and event config.

        Args:
            protocol (Protocol): Protocol instance
            network (Network): Network instance
            event_config (dict): Event configuration containing name, converter and model info

        Returns:
            Event: Created or existing Event instance
        """
        abi = protocol.get_evm_event_abi(event_config['name'])
        topic_0 = get_topic_0(abi)
        signature = get_signature(abi)

        event, is_created = Event.objects.update_or_create(
            name=event_config['name'],
            protocol_id=protocol.id,
            network_id=network.id,
            defaults={
                'topic_0': topic_0,
                'signature': signature,
                'abi': abi,
                'model_class': event_config['model_class'],
                'contract_addresses': contract_addresses
            }
        )

        if is_created:
            logger.info(
                f"Created Event instance: {event.name} for protocol "
                f"{protocol.name} on network {network.name}"
            )
        else:
            logger.info(
                f"Found existing Event instance: {event.name} for protocol "
                f"{protocol.name} on network {network.name}"
            )

        return event


InitializeAppTask = app.register_task(InitializeAppTask())


class ResetAppTask(Task):
    """Task to reset the app by deleting all Events, Networks and Protocols in reverse order."""

    def run(self):
        """Delete all Events, Networks and Protocols."""
        logger.info("Starting app reset...")

        ResetAssetsTask.run()

        # Delete all Events first since they depend on Networks and Protocols
        events_count = Event.objects.all().count()
        Event.objects.all().delete()
        logger.info(f"Deleted {events_count} Events")

        # Delete Networks next
        networks_count = Network.objects.all().count()
        Network.objects.all().delete()
        logger.info(f"Deleted {networks_count} Networks")

        # Delete Protocols last
        protocols_count = Protocol.objects.all().count()
        Protocol.objects.all().delete()
        logger.info(f"Deleted {protocols_count} Protocols")

        # Clear cache
        cache.clear()
        logger.info("Cache cleared")

        logger.info("App reset complete")


ResetAppTask = app.register_task(ResetAppTask())


class UpdateBlockNumberTask(Task):
    """Task to update the latest block number for a Network."""

    def run(self):
        """
        Updates the latest block number for a network by querying its RPC adapter.
        """
        logger.info("Starting block number update...")

        networks = Network.objects.all()

        for network in networks:
            try:
                block_height = network.rpc_adapter.block_height
                network.latest_block = block_height
                network.save()

                logger.info(f"Updated block number for {network.name} to {block_height}")
            except Exception as e:
                logger.error(f"Failed to update block number for {network.name}: {str(e)}")

        logger.info("Block number update complete")


UpdateBlockNumberTask = app.register_task(UpdateBlockNumberTask())


def group_events_by_network(events: List[Event]) -> Dict[Network, List[Event]]:
    """
    Groups a list of events by their associated network.

    Args:
        events (List[Event]): A list of Event objects to be grouped by network.

    Returns:
        Dict[Network, List[Event]]: A dictionary where the keys are Network objects
                                    and the values are lists of Event objects associated
                                    with each network.
    """
    events_by_network = defaultdict(list)
    for event in events:
        events_by_network[event.network].append(event)

    return events_by_network


def group_events_by_protocol(events: List[Event]) -> Dict[Protocol, List[Event]]:
    """
    Groups a list of events by their associated protocol.

    Args:
        events (List[Event]): A list of Event objects to be grouped by protocol.

    Returns:
        Dict[Protocol, List[Event]]: A dictionary where the keys are Protocol objects
                                     and the values are lists of Event objects associated
                                     with each protocol.
    """
    events_by_protocol = defaultdict(list)
    for event in events:
        events_by_protocol[event.protocol].append(event)
    return events_by_protocol


class BaseSynchronizeTask(Task):
    """
    Syncs event logs from contracts, signatures, and block ranges for a list of event IDs.
    """
    abstract = True
    expires = 1 * 60  # 1 minute in seconds
    time_limit = 1 * 60  # 1 minute in seconds

    def get_queryset(self, event_ids: List[int]):
        raise NotImplementedError

    def get_aave_pricesources(self, network: Network):
        return Asset.objects.filter(network=network).values_list('pricesource', flat=True)

    def get_aave_atokens(self, network: Network):
        return Asset.objects.filter(network=network).values_list('atoken_address', flat=True)

    def sync_events_for_network(self, network: Network, network_events: List[Event]):
        rpc_adapter = network.rpc_adapter
        global_to_block = rpc_adapter.block_height
        global_from_block = min(event.last_synced_block for event in network_events)

        if global_from_block >= global_to_block:
            logger.debug(f"{network.name} has no new blocks. Nothing to sync.")
            return

        iter_from_block = global_from_block
        iter_delta = min(global_to_block - global_from_block, rpc_adapter.max_blockrange_size_for_events)
        iter_to_block = global_from_block + iter_delta
        contract_addresses = list(set(
            Web3.to_checksum_address(address)
            for event in network_events
            for address in event.contract_addresses
        ))
        contract_addresses.extend([
            Web3.to_checksum_address(address)
            for address in self.get_aave_pricesources(network)
            if address
        ])
        contract_addresses.extend([
            Web3.to_checksum_address(address)
            for address in self.get_aave_atokens(network)
            if address
        ])

        EVENTS_ARRAY_THRESHOLD_SIZE = 5000

        while True:
            try:
                if (iter_from_block - iter_to_block) >= 0:
                    break

                logger.info(
                    f"Event Extraction for network {network.name} "
                    f"from {iter_from_block} to {iter_to_block}"
                )

                topics = [event.topic_0 for event in network_events]
                event_abis = {event.topic_0: event.abi for event in network_events}

                raw_event_dicts = rpc_adapter.extract_raw_event_data(
                    topics=topics,
                    contract_addresses=contract_addresses,
                    start_block=iter_from_block,
                    end_block=iter_to_block,
                )

                event_dicts = self.process_raw_event_dicts(raw_event_dicts, event_abis)
                # cleaned_event_dicts = self.clean_event_logs(network_events, event_dicts)
                self.handle_event_logs(network_events, event_dicts)

                if iter_to_block >= global_to_block:
                    self.update_last_synced_block(network_events, global_to_block)
                    logger.info(
                        f"Event Extraction for network {network.name} "
                        f"has completed from {global_from_block} to {global_to_block}"
                    )
                    break
                else:
                    iter_delta = min(iter_to_block - iter_from_block, rpc_adapter.max_blockrange_size_for_events)
                    self.update_last_synced_block(network_events, iter_to_block)
                    iter_from_block = iter_to_block

                    if len(event_dicts) >= EVENTS_ARRAY_THRESHOLD_SIZE:
                        iter_to_block += int(iter_delta / 2)
                    else:
                        iter_to_block += min(iter_delta * 2, rpc_adapter.max_blockrange_size_for_events)

                    if iter_to_block >= global_to_block:
                        iter_to_block = global_to_block

            except Web3RPCError as e:
                if e.rpc_response['error']['code'] == -32005:
                    logger.error(e)
                    iter_delta = iter_delta // 2
                    iter_to_block = iter_from_block + iter_delta

                    if iter_to_block >= global_to_block:
                        iter_to_block = global_to_block
                else:
                    raise e

    def process_raw_event_dicts(self, raw_event_dicts, event_abis):
        event_dicts = {}
        counter = 0
        codec = Web3().codec
        if not raw_event_dicts:
            return {}

        for log in raw_event_dicts:
            counter += 1
            topic0 = f"0x{log['topics'][0].hex()}"
            event_abi = event_abis.get(topic0)

            if event_abi:
                event_data = decode_any(get_event_data(codec, event_abi, log))
                if topic0 not in event_dicts:
                    event_dicts[topic0] = []
                event_dicts[topic0].append(event_data)
        return event_dicts

    def clean_event_logs(self, network_events, event_dicts):
        """
        Filters out events accidentally sent in or if they need to be
        filtered by contract address
        """
        cleaned_event_dicts = {}
        unique_log_comments = set()
        events = {event: event.topic_0 for event in network_events}

        for event, topic0 in events.items():
            cleaned_event_logs = []
            event_logs = event_dicts.get(topic0, {})

            contract_addresses = event.contract_addresses

            for event_log in event_logs:
                if not contract_addresses:
                    cleaned_event_logs.append(event_log)
                else:
                    if event_log['address'].lower() in contract_addresses:
                        cleaned_event_logs.append(event_log)
                    else:
                        comment = f"Filtered out log with address: {event_log['address']} for event {event.id}"
                        unique_log_comments.add(comment)

            cleaned_event_dicts[event.id] = cleaned_event_logs

            for comment in unique_log_comments:
                logger.info(comment)

        return cleaned_event_dicts

    def handle_event_logs(self, network_events: List[Event], cleaned_event_dicts: List[Dict]):
        """
        Saves a list of events to its respective model class
        """
        for network_event in network_events:
            adapter = network_event.get_adapter()
            func_name = f"parse_{network_event.name}"
            func = getattr(adapter, func_name)
            event_logs = cleaned_event_dicts.get(network_event.topic_0, [])
            if event_logs:
                func(network_event, event_logs)
                logger.info(f"Number of records inserted: {len(event_logs)}")

    def update_last_synced_block(self, events: List[Event], block: int):
        Event.objects.filter(id__in=[event.id for event in events]).update(
            last_synced_block=block, updated_at=datetime.now(pytz.utc)
        )


class BaseEventSynchronizeTask(BaseSynchronizeTask):
    abstract = True

    def run(self, event_ids: List[int]):
        events = self.get_queryset(event_ids=event_ids)
        events_by_network = group_events_by_network(events)

        for network, network_events in events_by_network.items():
            try:
                self.sync_events_for_network(network, network_events)
            except Exception as e:
                names = [event.name for event in network_events]
                logger.error(
                    f"Failed to sync events for network {network.name}: {str(e)}. Events: {names}",
                    exc_info=True
                )


class StreamingSynchronizeForEventTask(BaseEventSynchronizeTask):
    def get_queryset(self, event_ids: List[int]):
        return Event.objects.filter(
            id__in=event_ids,
            last_synced_block__gt=F("network__latest_block") - ACCEPTABLE_EVENT_BLOCK_LAG_DELAY,
            is_enabled=True
        )


StreamingSynchronizeForEventTask = app.register_task(
    StreamingSynchronizeForEventTask()
)


class BackfillSynchronizeForEventTask(BaseEventSynchronizeTask):
    def get_queryset(self, event_ids: List[int]):
        return Event.objects.filter(
            id__in=event_ids,
            last_synced_block__lte=F("network__latest_block") - ACCEPTABLE_EVENT_BLOCK_LAG_DELAY,
            is_enabled=True
        )


BackfillSynchronizeForEventTask = app.register_task(
    BackfillSynchronizeForEventTask()
)


class SynchronizeForEventTask(Task):
    def run(self, event_ids: List[int]):
        StreamingSynchronizeForEventTask.delay(event_ids)
        BackfillSynchronizeForEventTask.delay(event_ids)


SynchronizeForEventTask = app.register_task(SynchronizeForEventTask())


class BaseSynchronizeForGroupTask(Task):
    abstract = True

    @property
    def STREAMING_TASK(self):
        raise NotImplementedError

    @property
    def BACKFILL_TASK(self):
        raise NotImplementedError

    def get_queryset(self):
        raise NotImplementedError

    def run(self, *args):
        events = self.get_queryset(*args)
        event_pks = list(events.values_list('id', flat=True))
        self.STREAMING_TASK.delay(event_pks)

        events_by_network = group_events_by_network(events)

        for network, network_events in events_by_network.items():

            network_events = events.filter(network=network)
            grouped_events = group_events_by_protocol(network_events)
            for grouped_events in grouped_events.values():
                grouped_event_pks = [event.id for event in grouped_events]
                self.BACKFILL_TASK.delay(grouped_event_pks)


class SynchronizeForProtocolTask(BaseSynchronizeForGroupTask):
    STREAMING_TASK = StreamingSynchronizeForEventTask

    BACKFILL_TASK = BackfillSynchronizeForEventTask

    def get_queryset(self, *args):
        return Event.objects.filter(
            is_enabled=True,
            protocol__is_enabled=True,
            protocol_id__in=args[0]
        )


SynchronizeForProtocolTask = app.register_task(SynchronizeForProtocolTask())


class SynchronizeForAppTask(BaseSynchronizeForGroupTask):
    STREAMING_TASK = StreamingSynchronizeForEventTask

    BACKFILL_TASK = BackfillSynchronizeForEventTask

    def get_queryset(self):
        return Event.objects.filter(
            is_enabled=True,
            protocol__is_enabled=True
        )


SynchronizeForAppTask = app.register_task(SynchronizeForAppTask())


class UpdateMetadataCacheTask(Task):
    def run(self):
        logger.info("Starting metadata cache update...")

        logger.info("Updating network cache...")
        for network in Network.objects.all():
            logger.info(f"Caching network {network.name}")
            Network.get_network_by_name(network.name)
            Network.get_network_by_id(network.id)

        logger.info("Updating asset cache...")
        for asset in Asset.objects.all():
            logger.info(f"Caching asset {asset.asset} for {asset.protocol.name} on {asset.network.name}")
            Asset.get_by_address(asset.protocol.name, asset.network.name, asset.asset)
            Asset.get_by_id(asset.id)

        logger.info("Completed metadata cache update")


UpdateMetadataCacheTask = app.register_task(UpdateMetadataCacheTask())
