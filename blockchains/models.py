from django.db import models

from utils.rpc import rpc_adapter


class Event(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    last_synced_block = models.IntegerField(default=0)
    is_enabled = models.BooleanField(default=False)

    name = models.CharField(max_length=256, null=False)
    signature = models.CharField(max_length=1024, null=False)
    abi = models.JSONField(null=False)
    topic_0 = models.CharField(max_length=256, null=False)

    contract_addresses = models.JSONField(null=True)

    def __str__(self):
        return f"{self.name}"

    @property
    def blocks_to_sync(self):
        if self.last_synced_block is None:
            return None
        return rpc_adapter.cached_block_height - self.last_synced_block

    def _get_clickhouse_columns(self):
        return [
            (row["name"], self._map_evm_types_to_clickhouse_types(row["type"]))
            for row in self.abi["inputs"]
        ] + self._get_clickhouse_log_columns()

    def _map_evm_types_to_clickhouse_types(self, evm_type: str):
        if evm_type in [
            "uint256",
            "int256",
            "uint128",
            "int128",
            "uint64",
            "int64",
            "uint32",
            "int32",
            "uint16",
            "int16",
            "uint8",
            "int8",
        ]:
            return "UInt64"
        elif evm_type in ["bool"]:
            return "Int64"
        elif evm_type in ["address", "string"]:
            return "String"
        else:
            return "String"

    def _get_clickhouse_log_columns(self):
        return [
            ("address", "String"),
            ("blockNumber", "UInt64"),
            ("transactionHash", "String"),
            ("transactionIndex", "UInt64"),
            ("logIndex", "UInt64"),
        ]
