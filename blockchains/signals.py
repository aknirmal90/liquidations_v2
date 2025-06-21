import logging

from django.db.models.signals import pre_save

from blockchains.models import Event
from utils.clickhouse.client import clickhouse_client
from utils.explorers import token_metadata

logger = logging.getLogger(__name__)


def handle_events(sender, instance, **kwargs):
    logger.info(f"Handling event: {instance.name} (pk: {instance.pk})")

    pk = instance.pk
    if not pk:
        # If instance not been saved yet, skip
        return

    old_instance = Event.objects.get(pk=pk)
    if old_instance.logs_count == instance.logs_count:
        # If logs_count not changed, skip
        return

    logger.info(
        f"Logs count changed for event {pk}: {old_instance.logs_count} -> {instance.logs_count}"
    )

    if instance.name == "ReserveInitialized":
        logger.info("Processing ReserveInitialized event")
        handle_new_reserve(instance)


def handle_new_reserve(instance: Event):
    all_reserves_initialized_rows = clickhouse_client.select_event_rows(instance)
    all_assets = [row[0] for row in all_reserves_initialized_rows]

    assets_with_metadata_rows = clickhouse_client.select_rows("TokenMetadata")
    assets_with_metadata = [row[0] for row in assets_with_metadata_rows]

    new_metadata_count = 0
    for asset in all_assets:
        if asset not in assets_with_metadata:
            logger.info(f"Fetching metadata for asset: {asset}")
            try:
                metadata = token_metadata(asset)
                clickhouse_client.insert_rows(
                    "TokenMetadata", [[item for item in metadata.values()]]
                )
                new_metadata_count += 1
                logger.info(f"Successfully inserted metadata for asset: {asset}")
            except Exception as e:
                logger.error(f"Failed to fetch/insert metadata for asset {asset}: {e}")

    clickhouse_client.optimize_table("TokenMetadata")


pre_save.connect(handle_events, sender=Event)
