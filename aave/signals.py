import logging

from django.core.cache import cache
from django.core.serializers import serialize

from aave.models import Asset

logger = logging.getLogger(__name__)


def update_asset_cache(sender, instance, **kwargs):
    """
    Pre-save signal handler that updates the cache for an Asset instance.
    """
    key = Asset.get_cache_key_by_address(
        protocol_name=instance.protocol.name,
        network_name=instance.network.name,
        token_address=instance.asset
    )
    serialized_value = serialize("json", [instance])
    cache.set(key, serialized_value)

    key = instance.get_a_token_cache_key(
        protocol_id=instance.protocol.id,
        network_id=instance.network.id,
        atoken_address=instance.atoken_address
    )
    cache.set(key, instance.asset)
    logger.info(f"Successfully updated cache for asset {instance.asset}")
