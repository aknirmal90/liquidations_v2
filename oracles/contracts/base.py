import json
import logging
from datetime import datetime
from typing import Any, List

import requests
from django.core.cache import cache
from web3 import Web3

from utils.constants import NETWORK_NAME, PROTOCOL_NAME
from utils.encoding import decode_any
from utils.rpc import rpc_adapter

logger = logging.getLogger(__name__)


class BaseEthereumAssetSource:
    def __init__(self, asset: str, asset_source: str):
        self.asset = asset
        self.asset_source = asset_source

        if not self.name or not self.abi:
            name, abi = self.get_contract_info(asset_source)
            cache.set(self.local_cache_key("abi"), abi)
            cache.set(self.local_cache_key("name"), name)
            logger.info(f"Cached contract info for {asset_source} of asset {asset}")

    def get_contract_info(self, asset_source: str) -> tuple[str, str]:
        response = requests.get(
            f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={asset_source}&apikey=KNVMN27EBAAE1E9MUMX75N2QKFZWB2HB6J"
        )
        response = response.json()
        result = response["result"][0]
        return result["ContractName"], result["ABI"]

    def call_function(self, function_name: str, *args, **kwargs):
        if not hasattr(self, "contract"):
            self.contract = rpc_adapter.client.eth.contract(
                address=Web3.to_checksum_address(self.asset_source), abi=self.abi
            )

        return self.contract.functions[function_name](*args, **kwargs).call()

    @property
    def latest_price_from_rpc(self):
        return self.call_function("latestAnswer")

    def local_cache_key(self, function_name: str, *args, **kwargs):
        return f"{NETWORK_NAME}_{PROTOCOL_NAME}_{self.asset_source}_{function_name}"

    def global_cache_key(self, function_name: str, *args, **kwargs):
        return f"{NETWORK_NAME}_{PROTOCOL_NAME}_{function_name}"

    @property
    def abi(self):
        cache_key = self.local_cache_key("abi")
        abi = cache.get(cache_key)
        if abi is not None:
            return json.loads(abi) if isinstance(abi, str) else abi
        return None

    @property
    def name(self):
        cache_key = self.local_cache_key("name")
        return cache.get(cache_key)

    @property
    def events(self) -> List[str]:
        raise NotImplementedError

    @property
    def method_ids(self) -> List[str]:
        raise NotImplementedError

    def get_event_price(self, event: dict) -> int:
        raise NotImplementedError

    def get_transaction_price(self, transaction: dict) -> int:
        raise NotImplementedError

    def get_asset_sources_to_monitor(self) -> List[str]:
        raise NotImplementedError

    def process_event(self, event: dict) -> List[Any]:
        return [
            decode_any(self.asset),  # asset
            decode_any(self.asset_source),  # source
            self.get_event_price(event),  # price
            event.event,  # eventName
        ]

    def process_unconfirmed_transaction(self, transaction: dict) -> dict:
        return {
            "asset": decode_any(self.asset),
            "source": decode_any(self.asset_source),
            "price": self.get_transaction_price(transaction),
            "timestamp": int(datetime.now().timestamp()),
        }

    def _get_cached_property(self, property_name: str, function_name: str = None):
        """Helper method to get cached contract properties with consistent caching logic."""
        if function_name is None:
            function_name = property_name
        cache_key = self.local_cache_key(property_name)
        value = cache.get(cache_key)
        if value is None:
            value = self.call_function(function_name)
            cache.set(cache_key, value)
        return value
