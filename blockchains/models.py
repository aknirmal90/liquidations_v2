from django.core.cache import cache
from django.core.serializers import deserialize, serialize
from django.db import models

from utils.files import get_clazz_object, parse_json, parse_yaml
from utils.tokens import EvmTokenRetriever


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

    @property
    def transaction_adapter(self):
        adapter_path = f"{self.name}.adapter.TransactionAdapter"
        return get_clazz_object(adapter_path)(protocol_id=self.name)


class Network(models.Model):
    name = models.CharField(max_length=255, unique=True, null=False, blank=False, db_index=True)
    rpc = models.URLField()
    rpc_adapter_path = models.CharField(max_length=255)
    latest_block = models.BigIntegerField(null=True, blank=True, default=0)

    def __str__(self):
        return f"{self.name}"

    @property
    def rpc_adapter(self):
        return get_clazz_object(self.rpc_adapter_path)(self.name)

    @classmethod
    def get_cache_key(cls, network_name: str):
        return f"network-{network_name}"

    @classmethod
    def get_network(cls, network_name: str):
        if network_name is None:
            return

        key = cls.get_cache_key(network_name=network_name)
        serialized_value = cache.get(key)

        if serialized_value:
            return next(deserialize("json", serialized_value)).object
        else:
            network = cls.objects.get(network_name=network_name)
            deserialized_value = serialize("json", [network])
            cache.set(key, deserialized_value)
            return network


class Event(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    network = models.ForeignKey("blockchains.Network", null=False, on_delete=models.PROTECT)
    protocol = models.ForeignKey("blockchains.Protocol", null=False, on_delete=models.PROTECT)

    last_synced_block = models.IntegerField(default=0)
    is_enabled = models.BooleanField(default=False)

    event_name = models.CharField(max_length=256, null=False)
    event_signature = models.CharField(max_length=1024, null=False)
    event_abi = models.JSONField(null=False)
    event_topic_0 = models.CharField(max_length=256, null=False)

    def __str__(self):
        return f"{self.event_name} - {self.network}"

    @property
    def transaction_adapter(self):
        protocol = self.protocol
        adapter_path = f"{protocol.name}.adapter.TransactionAdapter"
        return get_clazz_object(adapter_path)

    @property
    def blocks_to_sync(self):
        if self.network.latest_block is None or self.last_synced_block is None:
            return None
        return self.network.latest_block - self.last_synced_block


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


class Token(models.Model):
    name = models.CharField(max_length=255, null=True, blank=True)
    token_address = models.CharField(max_length=255)
    network = models.ForeignKey(Network, on_delete=models.PROTECT)
    symbol = models.CharField(max_length=255, null=True, blank=True)
    decimals = models.IntegerField(null=True, blank=True)
    is_enabled = models.BooleanField(default=False)

    class Meta:
        unique_together = ('token_address', 'network')
        app_label = 'blockchains'

    def __str__(self):
        if self.symbol:
            return f"{self.symbol} on {self.network.name}"
        else:
            return f"{self.token_address} on {self.network.name}"

    @classmethod
    def get_cache_key(cls, network_name: str, token_address: str):
        return f"token-{network_name}-{token_address}"

    @classmethod
    def get(cls, network_name: str, token_address: str):
        if network_name is None or token_address is None:
            return

        key = cls.get_cache_key(network_name=network_name, token_address=token_address)
        serialized_value = cache.get(key)

        if serialized_value:
            return next(deserialize("json", serialized_value)).object
        else:
            try:
                token = cls.objects.get(token_address__iexact=token_address, network_name=network_name)
                deserialized_value = serialize("json", [token])
                cache.set(key, deserialized_value)
                return token
            except cls.DoesNotExist:
                token_retriever = EvmTokenRetriever(network_name=network_name, token_address=token_address)
                token_instance, is_created = Token.objects.get_or_create(
                    token_address=token_retriever.token_address,
                    network=token_retriever.network
                )
                token_instance.name = token_retriever.name
                token_instance.symbol = token_retriever.symbol
                token_instance.decimals = token_retriever.decimals
                token_instance.save()

                serialized_value = serialize("json", [token_instance])
                cache.set(key, serialized_value)
                return token_instance
