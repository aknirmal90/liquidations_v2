import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Type

import pytz
from django.core.cache import cache
from web3 import Web3
from web3._utils.events import get_event_data
from web3.exceptions import Web3RPCError

from utils.encoding import decode_any
from utils.rpc import get_evm_block_timestamps

logger = logging.getLogger(__name__)


class EventSynchronizeMixin:
    """
    Mixin for synchronizing event logs from contracts, signatures, and block ranges.
    Requires the inheriting class to define:
      - event_model: The Django model for the event (e.g., Event, PriceEvent)
      - clickhouse_client: The ClickHouse client instance
      - rpc_adapter: The RPC adapter instance
      - network_name: The network name string
    """

    event_model: Type[Any] = None
    clickhouse_client: Any = None
    rpc_adapter: Any = None
    network_name: str = None

    EVENTS_ARRAY_THRESHOLD_SIZE = 10_000
    EVENT_LOCK_TIMEOUT = 1800  # 30 minutes for individual event locks

    def run(self, event_ids: List[int]):
        """Default run method for child synchronize tasks."""
        self.run_event_sync(event_ids)

    def _acquire_event_locks(self, event_ids: List[int]) -> List[int]:
        """
        Acquire locks for individual events. Returns list of event_ids that were successfully locked.
        """
        locked_event_ids = []
        failed_locks = []

        for event_id in event_ids:
            lock_key = f"event_sync_lock_{self.__class__.__name__}_{event_id}"
            lock_acquired = cache.add(lock_key, "locked", self.EVENT_LOCK_TIMEOUT)

            if lock_acquired:
                locked_event_ids.append(event_id)
            else:
                failed_locks.append(event_id)
                logger.warning(
                    f"Event {event_id} already being synced by another task, skipping"
                )

        if failed_locks:
            logger.info(
                f"Could not acquire locks for {len(failed_locks)} events: {failed_locks}"
            )

        if locked_event_ids:
            logger.info(
                f"Successfully acquired locks for {len(locked_event_ids)} events: {locked_event_ids}"
            )

        return locked_event_ids

    def _release_event_locks(self, event_ids: List[int]):
        """Release locks for the given event IDs."""
        for event_id in event_ids:
            lock_key = f"event_sync_lock_{self.__class__.__name__}_{event_id}"
            cache.delete(lock_key)
        logger.debug(f"Released locks for {len(event_ids)} events")

    def run_event_sync(self, event_ids: List[int]):
        # First, try to acquire locks for individual events
        locked_event_ids = self._acquire_event_locks(event_ids)

        if not locked_event_ids:
            logger.warning(
                f"Could not acquire locks for any events in {event_ids}, skipping sync"
            )
            return

        try:
            # Only process events that we successfully locked
            network_events = self.event_model.objects.filter(id__in=locked_event_ids)
            if not network_events.exists():
                logger.warning(
                    f"No events found for locked event_ids {locked_event_ids}"
                )
                return

            global_to_block = self.rpc_adapter.block_height
            global_from_block = min(event.last_synced_block for event in network_events)
            if global_from_block != 0:
                global_from_block += 1

            if global_from_block >= global_to_block:
                logger.debug(f"{self.network_name} has no new blocks. Nothing to sync.")
                return

            iter_from_block = global_from_block
            iter_delta = min(
                global_to_block - global_from_block,
                self.rpc_adapter.max_blockrange_size_for_events,
            )
            iter_to_block = global_from_block + iter_delta
            contract_addresses = list(
                set(
                    Web3.to_checksum_address(address)
                    for event in network_events
                    for address in event.contract_addresses
                )
            )

            while True:
                try:
                    if (iter_from_block - iter_to_block) >= 0:
                        break

                    logger.info(
                        f"Event Extraction for network {self.network_name} "
                        f"from {iter_from_block} to {iter_to_block}"
                    )

                    topics = [event.topic_0 for event in network_events]
                    event_abis = {event.topic_0: event.abi for event in network_events}

                    raw_event_dicts = self.rpc_adapter.extract_raw_event_data(
                        topics=topics,
                        contract_addresses=contract_addresses,
                        start_block=iter_from_block,
                        end_block=iter_to_block,
                    )

                    event_dicts = self.process_raw_event_dicts(
                        raw_event_dicts=raw_event_dicts, event_abis=event_abis
                    )
                    self.handle_event_logs(
                        network_events=network_events, event_dicts=event_dicts
                    )
                    self.post_handle_hook(
                        network_events=network_events,
                        start_block=iter_from_block,
                        end_block=iter_to_block,
                    )

                    if iter_to_block >= global_to_block:
                        self.update_last_synced_block(
                            network_events, global_to_block, locked_event_ids
                        )
                        logger.info(
                            f"Event Extraction for network {self.network_name} "
                            f"has completed from {global_from_block} to {global_to_block}"
                        )
                        break
                    else:
                        iter_delta = min(
                            iter_to_block - iter_from_block,
                            self.rpc_adapter.max_blockrange_size_for_events,
                        )
                        self.update_last_synced_block(
                            network_events, iter_to_block, locked_event_ids
                        )
                        iter_from_block = iter_to_block + 1

                        if len(event_dicts) >= self.EVENTS_ARRAY_THRESHOLD_SIZE:
                            iter_to_block += int(iter_delta / 2)
                        else:
                            iter_to_block += min(
                                iter_delta * 2,
                                self.rpc_adapter.max_blockrange_size_for_events,
                            )

                        if iter_to_block >= global_to_block:
                            iter_to_block = global_to_block

                except Web3RPCError as e:
                    if e.rpc_response["error"]["code"] == -32005:
                        logger.info(e)
                        iter_delta = iter_delta // 2
                        iter_to_block = iter_from_block + iter_delta

                        if iter_to_block >= global_to_block:
                            iter_to_block = global_to_block
                    else:
                        raise e
        finally:
            # Always release the locks for the events we processed
            self._release_event_locks(locked_event_ids)

    def post_handle_hook(
        self, network_events: List[Any], start_block: int, end_block: int
    ):
        return

    def process_raw_event_dicts(self, raw_event_dicts, event_abis):
        event_dicts = {}
        codec = Web3().codec
        if not raw_event_dicts:
            return {}

        for log in raw_event_dicts:
            topic0 = f"0x{log['topics'][0].hex()}"
            event_abi = event_abis.get(topic0)

            if event_abi:
                event_data = decode_any(get_event_data(codec, event_abi, log))
                if topic0 not in event_dicts:
                    event_dicts[topic0] = []
                event_dicts[topic0].append(event_data)
        return event_dicts

    def get_timestamps_for_events(self, event_logs: List[Any]):
        blocks = list(set([event.blockNumber for event in event_logs]))
        return get_evm_block_timestamps(blocks)

    def handle_event_logs(self, network_events: List[Any], event_dicts: Dict):
        for network_event in network_events:
            event_logs = event_dicts.get(network_event.topic_0, [])
            log_fields = [
                col_name for col_name, _ in network_event._get_clickhouse_log_columns()
            ]
            all_fields = [
                col_name for col_name, _ in network_event._get_clickhouse_columns()
            ]
            arg_fields = [field for field in all_fields if field not in log_fields]

            if not event_logs:
                continue

            timestamps = self.get_timestamps_for_events(event_logs)
            parsed_event_logs = []

            for event_log in event_logs:
                event_log_args = getattr(event_log, "args")
                event_values = [getattr(event_log_args, field) for field in arg_fields]
                log_values = [
                    getattr(event_log, field)
                    for field in log_fields
                    if field != "blockTimestamp"
                ]
                log_values.append(timestamps[event_log.blockNumber])
                parsed_event_log = event_values + log_values
                parsed_event_logs.append(parsed_event_log)

            for i in range(3):
                try:
                    self.clickhouse_client.insert_event_logs(
                        network_event, parsed_event_logs
                    )
                    break
                except Exception as e:
                    logger.error(f"Error inserting rows: {e}")
                    time.sleep(5)

            for i in range(3):
                try:
                    self.clickhouse_client.optimize_table(network_event.name)
                    break
                except Exception as e:
                    logger.error(f"Error optimizing table: {e}")
                    time.sleep(5)

            network_event.logs_count += len(event_logs)
            network_event.save()
            logger.info(f"Number of records inserted: {len(event_logs)}")

    def update_last_synced_block(
        self, events: List[Any], block: int, locked_event_ids: List[int] = None
    ):
        """
        Update last_synced_block for events. If locked_event_ids is provided,
        only update events that have locks obtained.
        """
        if locked_event_ids is not None:
            # Only update events that have locks obtained
            event_ids_to_update = [
                event.id for event in events if event.id in locked_event_ids
            ]
        else:
            # Fallback to updating all events (for backward compatibility)
            event_ids_to_update = [event.id for event in events]

        if event_ids_to_update:
            self.event_model.objects.filter(id__in=event_ids_to_update).update(
                last_synced_block=block, updated_at=datetime.now(pytz.utc)
            )


class ParentSynchronizeTaskMixin:
    """
    Mixin for parent synchronize tasks. Groups events by last_synced_block and fires child tasks.
    Requires inheriting class to define:
      - event_model: The Django model for the event (e.g., Event, PriceEvent)
      - child_task: The celery child task to call (must have .delay method)
    """

    event_model: Type[Any] = None
    child_task: Any = None

    # Lock configuration
    LOCK_TIMEOUT = 300  # 5 minutes

    def run(self):
        """Default run method for parent synchronize tasks."""
        self.run_parent_sync()

    def run_parent_sync(self):
        # Create a unique lock key for this parent task class
        lock_key = f"parent_sync_lock_{self.__class__.__name__}"

        # Try to acquire lock to prevent concurrent execution
        lock_acquired = cache.add(lock_key, "locked", self.LOCK_TIMEOUT)
        if not lock_acquired:
            logger.warning(
                f"Parent sync {self.__class__.__name__} already running, skipping execution"
            )
            return

        try:
            logger.info(f"Starting parent sync for {self.__class__.__name__}")
            events = self.event_model.objects.filter(is_enabled=True)
            events_by_block = {}
            for event in events.iterator():
                block = event.last_synced_block
                if block not in events_by_block:
                    events_by_block[block] = []
                events_by_block[block].append(event.id)

            for block, event_ids in events_by_block.items():
                self.child_task.delay(event_ids=event_ids)

            logger.info(
                f"Parent sync {self.__class__.__name__} completed, spawned {len(events_by_block)} child tasks"
            )

        finally:
            # Always release the lock
            cache.delete(lock_key)
            logger.debug(f"Released parent sync lock for {self.__class__.__name__}")
