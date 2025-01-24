import pytest
from django.core.cache import cache
from django.core.serializers import serialize

from blockchains.models import Network, Protocol


@pytest.fixture
def protocol():
    return Protocol.objects.create(
        name="test_protocol",
        is_enabled=True
    )


@pytest.fixture
def network():
    return Network.objects.create(name="test_network")


def compare_all_model_fields(instance_a, instance_b):
    """
    Compare field-by-field for two model instances to ensure they match.
    """
    # Both instances should be of the same model class
    assert instance_a.__class__ == instance_b.__class__

    for field in instance_a._meta.get_fields():
        # Skip reverse accessor fields
        if field.auto_created and not field.concrete:
            continue

        field_name = field.name

        # Compare the field values
        # For relation fields, Django will store _id if the field is an FK, etc.
        value_a = getattr(instance_a, field_name)
        value_b = getattr(instance_b, field_name)
        assert value_a == value_b, f"Mismatch in field '{field_name}': {value_a} != {value_b}"


@pytest.mark.django_db
class TestSignals:
    def test_protocol_cache_update_on_save(self, protocol):
        # Get the cache key
        cache_key = Protocol.get_cache_key(protocol_name=protocol.name)

        # (1) Verify the cache was set
        cached_protocol = Protocol.get_protocol_by_name(protocol_name=protocol.name)
        compare_all_model_fields(cached_protocol, protocol)

        # (2) Update the protocol
        protocol.refresh_from_db()
        protocol.is_enabled = False
        protocol.save()

        # (3) Check the raw cached JSON string, if you want that test
        cached_json = cache.get(cache_key)
        assert cached_json == serialize("json", [protocol])

        # (4) Confirm deserialized version also matches
        updated_cached_protocol = Protocol.get_protocol_by_name(protocol.name)
        compare_all_model_fields(updated_cached_protocol, protocol)

    def test_network_cache_update_on_save(self, network):
        # Get the cache key
        cache_key = Network.get_cache_key_by_name(network_name=network.name)

        # (1) Verify the cache was set
        cached_network = Network.get_network_by_name(network_name=network.name)
        compare_all_model_fields(cached_network, network)

        # (2) Update the network
        network.rpc = "https://updated.rpc"
        network.save()

        # (3) Check the raw cached JSON string
        cached_json = cache.get(cache_key)
        assert cached_json == serialize("json", [network])

        # (4) Confirm deserialized version also matches
        updated_cached_network = Network.get_network_by_name(network.name)
        compare_all_model_fields(updated_cached_network, network)

    def teardown_method(self):
        # Clear cache after each test
        cache.clear()
