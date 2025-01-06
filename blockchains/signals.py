from django.core.cache import cache
from django.core.serializers import serialize

from blockchains.models import Network, Protocol


def update_protocol_cache(sender, instance, **kwargs):
    key = Protocol.get_cache_key(protocol_name=instance.name)
    serialized_value = serialize("json", [instance])
    cache.set(key, serialized_value)


def update_network_cache(sender, instance, **kwargs):
    key = Network.get_cache_key_by_name(network_name=instance.name)
    serialized_value = serialize("json", [instance])
    cache.set(key, serialized_value)
