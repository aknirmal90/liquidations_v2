import logging
import time
from typing import Any, Dict, List

from celery import Task

from balances.models import BalanceEvent
from liquidations_v2.celery_app import app
from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_NAME
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
                    self.clickhouse_client.optimize_table("Balances")
                    break
                except Exception as e:
                    logger.error(f"Error optimizing table: {e}")
                    time.sleep(5)

                logger.info(f"Number of records inserted: {len(parsed_event_logs)}")

            BalanceEvent.objects.bulk_update(updated_network_events, ["logs_count"])


ChildBalancesSynchronizeTask = app.register_task(ChildBalancesSynchronizeTask())


class ParentBalancesSynchronizeTask(ParentSynchronizeTaskMixin, Task):
    event_model = BalanceEvent
    child_task = ChildBalancesSynchronizeTask


ParentBalancesSynchronizeTask = app.register_task(ParentBalancesSynchronizeTask())
