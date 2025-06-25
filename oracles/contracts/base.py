import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import requests
from django.core.cache import cache
from web3 import Web3
from web3.datastructures import AttributeDict

from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_NAME, PRICE_CACHE_EXPIRY, PROTOCOL_NAME
from utils.encoding import decode_any
from utils.rpc import rpc_adapter

logger = logging.getLogger(__name__)


class RatioProviderMixin:
    """Mixin providing common ratio provider functionality for price adapters."""

    @property
    def RATIO_PROVIDER_METHOD(self):
        """The method name to call on the ratio provider contract."""
        raise NotImplementedError("RATIO_PROVIDER_METHOD must be implemented")

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        """The cached property name for the ratio provider address."""
        return "RATIO_PROVIDER"

    @property
    def RATIO_PROVIDER(self):
        """Get the ratio provider contract address from cache."""
        return self._get_cached_property(self.RATIO_PROVIDER_ADDRESS_NAME)

    @property
    def RATIO_DECIMALS(self):
        """Get the ratio decimals from cache."""
        return self._get_cached_property("RATIO_DECIMALS")

    def get_base_abi(self):
        """Get the base ABI for ratio provider methods that take no parameters."""
        abi = [
            {
                "inputs": [],
                "name": self.RATIO_PROVIDER_METHOD,
                "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function",
            }
        ]
        return abi

    @property
    def RATIO_PROVIDER_ABI(self):
        """Get the ABI for the ratio provider contract. Override in subclasses if needed."""
        return self.get_base_abi()

    def get_ratio(self, use_parameter=False, parameter=None):
        """
        Get the current ratio from the ratio provider contract.

        Args:
            use_parameter: Whether the method call requires a parameter
            parameter: The parameter to pass to the method (defaults to 10**RATIO_DECIMALS)
        """
        cache_key = self.local_cache_key("ratio")
        ratio = cache.get(cache_key)

        if ratio is None:
            contract = rpc_adapter.client.eth.contract(
                address=Web3.to_checksum_address(self.RATIO_PROVIDER),
                abi=self.RATIO_PROVIDER_ABI,
            )
            func = getattr(contract.functions, self.RATIO_PROVIDER_METHOD)

            if use_parameter:
                if parameter is None:
                    parameter = 10**self.RATIO_DECIMALS
                ratio = func(parameter).call()
            else:
                ratio = func().call()

            cache.set(cache_key, ratio, PRICE_CACHE_EXPIRY)
        return ratio


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

        logger.info(
            f"Calling function {function_name} with args {args} and kwargs {kwargs}"
        )
        return self.contract.functions[function_name](*args, **kwargs).call()

    @property
    def latest_price_from_rpc(self):
        return self.call_function("latestAnswer")

    @property
    def latest_price_from_clickhouse(self):
        query = f"""
        SELECT price
        FROM aave_ethereum.LatestRawPriceEvent
        WHERE asset = '{self.asset}'
        ORDER BY blockTimestamp DESC
        LIMIT 1
        """
        result = clickhouse_client.execute_query(query)
        return result.result_rows[0][0]

    @property
    def latest_price_from_postgres(self):
        return self.process_current_price()[2]

    def local_cache_key(self, function_name: str, *args, **kwargs):
        return f"{NETWORK_NAME}_{PROTOCOL_NAME}_{self.asset_source.lower()}_{function_name}"

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

    def get_underlying_sources_to_monitor(self) -> List[str]:
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

    def _get_cached_property(
        self, property_name: str, function_name: str = None, expiry: int = None
    ):
        """Helper method to get cached contract properties with consistent caching logic."""
        if function_name is None:
            function_name = property_name
        cache_key = self.local_cache_key(property_name)
        value = cache.get(cache_key)

        if value is None:
            value = self.call_function(function_name)
            if expiry is None:
                cache.set(cache_key, value)
            else:
                cache.set(cache_key, value, expiry)
        return value

    def process_current_price(self) -> Dict[str, Any]:
        price = cache.get(self.local_cache_key("underlying_price"))
        if price is None:
            price = self.latest_price_from_rpc
            cache.set(self.local_cache_key("underlying_price"), price)

        synthetic_event = AttributeDict(
            {
                "args": AttributeDict({"answer": price}),
                "event": "SyntheticPriceEvent",
                "transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "logIndex": 0,
                "blockNumber": rpc_adapter.cached_block_height,
                "address": self.get_underlying_sources_to_monitor()[0],
                "topics": [],
            }
        )
        return self.process_event(synthetic_event)
