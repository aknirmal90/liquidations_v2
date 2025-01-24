from typing import Any, Dict, List

from django.core.cache import cache
from django.core.serializers import deserialize, serialize
from django.db import models

from utils.exceptions import ABINotFoundError, ConfigFileNotFoundError, EventABINotFoundError
from utils.files import get_clazz_object, parse_json, parse_yaml
from utils.rpc import EVMRpcAdapter


class Protocol(models.Model):
    name = models.CharField(max_length=256, null=False, blank=False, unique=True)
    is_enabled = models.BooleanField(default=False)

    def __str__(self) -> str:
        return f"{self.name}"

    @property
    def config_path(self) -> str:
        return f"{self.name}/config.yaml"

    @property
    def config(self) -> Any | None:
        try:
            return parse_yaml(file_path=self.config_path)
        except FileNotFoundError:
            raise ConfigFileNotFoundError(file_path=self.config_path)

    @property
    def evm_abi(self) -> Any | None:
        abi_path = f"{self.name}/abi.json"
        try:
            return parse_json(file_path=abi_path)
        except FileNotFoundError:
            raise ABINotFoundError(file_path=abi_path)

    def get_evm_event_abi(self, name: str) -> None | Any:
        for item in self.evm_abi:
            if item.get("type") == "event" and item.get("name") == name:
                return item

        raise EventABINotFoundError(event_name=name)

    @classmethod
    def get_cache_key(cls, protocol_name: str) -> str:
        return f"protocol-{protocol_name}"

    @classmethod
    def get_protocol_by_name(cls, protocol_name: str) -> None | Any:
        """
        Always return the protocol instance from cache (deserialized).
        """
        if not protocol_name:
            return None

        key = cls.get_cache_key(protocol_name=protocol_name)
        serialized_value = cache.get(key=key)

        if not serialized_value:
            # If not in cache, fetch from DB and store it
            protocol = cls.objects.get(name=protocol_name)
            serialized_value = serialize(format="json", queryset=[protocol])
            cache.set(key=key, value=serialized_value)

        # Always deserialize to return the same 'cached style' object
        return next(deserialize(format="json", stream_or_string=serialized_value)).object


class Network(models.Model):
    name = models.CharField(max_length=255, unique=True, null=False, blank=False, db_index=True)
    rpc = models.URLField(null=True, blank=True)
    chain_id = models.IntegerField(null=True, blank=True)

    wss_infura = models.CharField(max_length=255, null=True, blank=True)
    wss_sequencer_oregon = models.CharField(max_length=255, null=True, blank=True)

    latest_block = models.BigIntegerField(null=True, blank=True, default=0)

    def __str__(self) -> str:
        return f"{self.name}"

    @property
    def rpc_adapter(self) -> EVMRpcAdapter:
        from utils.rpc import get_adapters
        return get_adapters()[self.name]

    @classmethod
    def get_cache_key_by_name(cls, network_name: str) -> str:
        return f"network-{network_name}"

    @classmethod
    def get_cache_key_by_id(cls, id: int) -> str:
        return f"network-{id}"

    @classmethod
    def get_network_by_name(cls, network_name: str) -> None | Any:
        """
        Unified approach that always returns from deserialized cache object.
        """
        if not network_name:
            return None

        key = cls.get_cache_key_by_name(network_name=network_name)
        serialized_value = cache.get(key=key)

        if not serialized_value:
            network = cls.objects.get(name=network_name)
            serialized_value = serialize(format="json", queryset=[network])
            cache.set(key=key, value=serialized_value)

        return next(deserialize(format="json", stream_or_string=serialized_value)).object

    @classmethod
    def get_network_by_id(cls, id: int):
        """
        Unified approach that always returns from deserialized cache object.
        `id` here refers to the primary key of the network.
        """
        if id is None:
            return None

        key = cls.get_cache_key_by_id(id=id)
        serialized_value = cache.get(key=key)

        if not serialized_value:
            network = cls.objects.get(id=id)
            serialized_value = serialize(format="json", queryset=[network])
            cache.set(key=key, value=serialized_value)

        return next(deserialize(format="json", stream_or_string=serialized_value)).object


class Event(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    network = models.ForeignKey("blockchains.Network", null=False, on_delete=models.PROTECT)
    protocol = models.ForeignKey("blockchains.Protocol", null=False, on_delete=models.PROTECT)

    last_synced_block = models.IntegerField(default=0)
    is_enabled = models.BooleanField(default=False)

    name = models.CharField(max_length=256, null=False)
    signature = models.CharField(max_length=1024, null=False)
    abi = models.JSONField(null=False)
    topic_0 = models.CharField(max_length=256, null=False)

    model_class = models.CharField(max_length=256, null=True)
    contract_addresses = models.JSONField(null=True)

    def __str__(self):
        return f"{self.name} - {self.network}"

    @property
    def blocks_to_sync(self):
        if self.network.latest_block is None or self.last_synced_block is None:
            return None
        return self.network.latest_block - self.last_synced_block

    def get_model_class(self):
        return get_clazz_object(absolute_path=self.model_class)

    def get_adapter(self):
        from utils.protocols import get_adapters
        return get_adapters()[self.protocol.name]


class ApproximateBlockTimestamp(models.Model):
    reference_block_number = models.BigIntegerField(db_index=True)
    timestamp = models.BigIntegerField(null=True, blank=True)
    network = models.ForeignKey(Network, on_delete=models.CASCADE, unique=True)
    block_time_in_milliseconds = models.BigIntegerField(null=True, blank=True)

    class Meta:
        app_label = 'blockchains'

    def __str__(self):
        return f"{self.network.name} - {self.reference_block_number}"

    def get_timestamps(self, blocks: List[int]) -> Dict[int, int]:
        return {
            block_number: int(self.timestamp + (
                block_number - self.reference_block_number
            ) * self.block_time_in_milliseconds / 1_000)
            for block_number in blocks
        }
