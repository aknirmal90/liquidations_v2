from typing import Dict, List

from django.core.cache import cache
from django.core.serializers import deserialize, serialize
from django.db import models

from utils.files import get_clazz_object, parse_json, parse_yaml


class Protocol(models.Model):
    name = models.CharField(max_length=256, null=False, blank=False, unique=True)
    is_enabled = models.BooleanField(default=False)

    def __str__(self) -> str:
        return f"{self.name}"

    @property
    def config_path(self):
        return f"{self.name}/config.yaml"

    @property
    def config(self):
        return parse_yaml(self.config_path)

    @property
    def evm_abi(self):
        abi_path = f"{self.name}/abi.json"
        return parse_json(abi_path)

    def get_evm_event_abi(self, name: str):
        if not name:
            return

        for item in self.evm_abi:
            if item.get("type") == "event" and item.get("name") == name:
                return item

        raise ValueError(f"ABI not found for Name: {name}")

    @classmethod
    def get_cache_key(cls, protocol_name: str):
        return f"protocol-{protocol_name}"

    @classmethod
    def get_protocol_by_name(cls, protocol_name: str):
        """
        Always return the protocol instance from cache (deserialized).
        """
        if not protocol_name:
            return None

        key = cls.get_cache_key(protocol_name=protocol_name)
        serialized_value = cache.get(key)

        if not serialized_value:
            # If not in cache, fetch from DB and store it
            protocol = cls.objects.get(name=protocol_name)
            serialized_value = serialize("json", [protocol])
            cache.set(key, serialized_value)

        # Always deserialize to return the same 'cached style' object
        return next(deserialize("json", serialized_value)).object


class Network(models.Model):
    name = models.CharField(max_length=255, unique=True, null=False, blank=False, db_index=True)
    rpc = models.URLField(null=True, blank=True)
    chain_id = models.IntegerField(null=True, blank=True)

    wss_infura = models.CharField(max_length=255, null=True, blank=True)
    wss_sequencer_oregon = models.CharField(max_length=255, null=True, blank=True)

    latest_block = models.BigIntegerField(null=True, blank=True, default=0)

    def __str__(self):
        return f"{self.name}"

    @property
    def rpc_adapter(self):
        from utils.rpc import Adapters as NetworkAdapters
        return NetworkAdapters[self.name]

    @classmethod
    def get_cache_key_by_name(cls, network_name: str):
        return f"network-{network_name}"

    @classmethod
    def get_cache_key_by_id(cls, id: int):
        return f"network-{id}"

    @classmethod
    def get_network_by_name(cls, network_name: str):
        """
        Unified approach that always returns from deserialized cache object.
        """
        if not network_name:
            return None

        key = cls.get_cache_key_by_name(network_name=network_name)
        serialized_value = cache.get(key)

        if not serialized_value:
            network = cls.objects.get(name=network_name)
            serialized_value = serialize("json", [network])
            cache.set(key, serialized_value)

        return next(deserialize("json", serialized_value)).object

    @classmethod
    def get_network_by_id(cls, id: int):
        """
        Unified approach that always returns from deserialized cache object.
        """
        if id is None:
            return None

        key = cls.get_cache_key_by_id(id=id)
        serialized_value = cache.get(key)

        if not serialized_value:
            network = cls.objects.get(id=id)
            serialized_value = serialize("json", [network])
            cache.set(key, serialized_value)

        return next(deserialize("json", serialized_value)).object


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
        return get_clazz_object(self.model_class)

    def get_adapter(self):
        from utils.protocols import Adapters as ProtocolAdapters
        return ProtocolAdapters[self.protocol.name]


class Contract(models.Model):
    contract_address = models.CharField(max_length=256, null=False)
    network = models.ForeignKey(
        "blockchains.Network",
        null=True,
        on_delete=models.PROTECT,
        related_name="contracts"
    )
    protocol = models.ForeignKey(
        "blockchains.Protocol",
        null=True,
        on_delete=models.PROTECT,
        related_name="contracts"
    )
    is_enabled = models.BooleanField(default=True)

    def __str__(self) -> str:
        return f"{self.contract_address} for {self.protocol} on {self.network}"

    @property
    def abi(self):
        return self.protocol.abi

    @classmethod
    def get_cache_key(cls, network_name: str, contract_address: str, protocol_id: int):
        return f"contract-{network_name}-{contract_address}-{protocol_id}"

    @classmethod
    def get(cls, network_name: str, contract_address: str, protocol_id: int):
        if network_name is None or contract_address is None or protocol_id is None:
            return

        key = cls.get_cache_key(
            network_name=network_name,
            contract_address=contract_address,
            protocol_id=protocol_id
        )
        serialized_value = cache.get(key)

        if serialized_value:
            return next(deserialize("json", serialized_value)).object
        else:
            try:
                contract = cls.objects.get(
                    contract_address=contract_address,
                    network__network_name=network_name,
                    protocol_id=protocol_id
                )
                deserialized_value = serialize("json", [contract])
                cache.set(key, deserialized_value)
                return contract
            except cls.DoesNotExist:
                network = Network.objects.get(network_name=network_name)
                contract_instance, is_created = Contract.objects.get_or_create(
                    network=network,
                    contract_address=contract_address,
                    protocol_id=protocol_id
                )
                serialized_value = serialize("json", [contract_instance])
                cache.set(key, serialized_value)
                return contract_instance


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
