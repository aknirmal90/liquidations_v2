from django.core.cache import cache
from django.core.serializers import serialize


def update_protocol_cache(sender, instance, **kwargs):
    from blockchains.models import Protocol

    key = Protocol.get_cache_key(protocol_name=instance.name)
    serialized_value = serialize(format="json", queryset=[instance])
    cache.set(key=key, value=serialized_value)


def update_network_cache(sender, instance, **kwargs):
    from blockchains.models import Network

    # Update cache by name
    key = Network.get_cache_key_by_name(network_name=instance.name)
    serialized_value = serialize(format="json", queryset=[instance])
    cache.set(key=key, value=serialized_value)

    # Update cache by id
    key_by_id = Network.get_cache_key_by_id(id=instance.id)
    cache.set(key=key_by_id, value=serialized_value)
