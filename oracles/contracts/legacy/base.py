import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import web3
from django.core.cache import cache
from web3 import Web3

from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_NAME, PRICE_CACHE_EXPIRY, PROTOCOL_NAME
from utils.encoding import decode_any
from utils.rpc import get_evm_block_timestamps, rpc_adapter

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

    def get_ratio(
        self, use_parameter=False, parameter=None, block_number: Optional[int] = None
    ):
        """
        Get the current ratio from the ratio provider contract.

        Args:
            use_parameter: Whether the method call requires a parameter
            parameter: The parameter to pass to the method (defaults to 10**RATIO_DECIMALS)
            block_number: Optional block number to query the ratio at
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
                if block_number is not None:
                    ratio = func(parameter).call(block_identifier=block_number)
                else:
                    ratio = func(parameter).call()
            else:
                if block_number is not None:
                    ratio = func().call(block_identifier=block_number)
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
        if isinstance(result, str):
            logger.info(f"ABI is a string for {asset_source}")
        return result["ContractName"], result["ABI"]

    def call_function(
        self, function_name: str, *args, block_number: Optional[int] = None, **kwargs
    ):
        if not hasattr(self, "contract"):
            self.contract = rpc_adapter.client.eth.contract(
                address=Web3.to_checksum_address(self.asset_source), abi=self.abi
            )

        logger.info(
            f"Calling function {function_name} with args {args} and kwargs {kwargs}"
            + (f" at block {block_number}" if block_number is not None else "")
        )

        # Get the function and prepare the call
        func = self.contract.functions[function_name](*args, **kwargs)

        # Call with block number if specified, otherwise call normally
        if block_number is not None:
            return func.call(block_identifier=block_number)
        else:
            return func.call()

    @property
    def latest_price_from_rpc(self):
        return self.call_function("latestAnswer")

    @property
    def historical_price_from_event(self):
        query = f"""
        SELECT historical_price
        FROM aave_ethereum.LatestPriceEvent
        WHERE asset = '{self.asset}'
        ORDER BY timestamp DESC
        LIMIT 1
        """
        result = clickhouse_client.execute_query(query)
        return result.result_rows[0][0]

    @property
    def predicted_price(self):
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
        # NewTransmission (
        # index_topic_1
        # uint32 aggregatorRoundId,
        # int192 answer,
        # address transmitter,
        # uint32 observationsTimestamp,
        # int192[] observations,
        # bytes observers,
        # int192 juelsPerFeeCoin,
        # bytes32 configDigest,
        # uint40 epochAndRound
        # )
        return [
            "NewTransmission",
        ]

    @property
    def method_ids(self) -> List[str]:
        # forward(address to, bytes data)
        return [
            "0x6fadcf72",
        ]

    def get_underlying_sources_to_monitor(self) -> List[str]:
        raise NotImplementedError

    # ============================================================================
    # Core Price Calculation Methods - Override these for custom logic
    # ============================================================================

    def get_numerator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the numerator for price calculation.
        Override this method to provide custom numerator logic.

        Args:
            event: Event dictionary (for get_event_price)
            transaction: Transaction dictionary (for get_transaction_price)

        Returns:
            Numerator value as integer
        """
        raise NotImplementedError("get_numerator must be implemented")

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the denominator for price calculation.
        Override this method to provide custom denominator logic.
        Default implementation returns 1 (no division).

        Args:
            event: Event dictionary (for get_event_price)
            transaction: Transaction dictionary (for get_transaction_price)

        Returns:
            Denominator value as integer (default: 1)
        """
        return 1

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Override this method to provide custom multiplier logic.
        Default implementation returns 1 (no multiplication).

        Args:
            event: Event dictionary (for get_event_price)
            transaction: Transaction dictionary (for get_transaction_price)

        Returns:
            Multiplier value as integer (default: 1)
        """
        return 1

    def get_timestamp(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the timestamp for the price calculation.
        Override this method to provide custom timestamp logic.

        Args:
            event: Event dictionary (for get_event_price)
            transaction: Transaction dictionary (for get_transaction_price)

        Returns:
            Timestamp as integer
        """
        if event:
            block_number = getattr(event, "blockNumber")
            block_timestamp = get_evm_block_timestamps([block_number])[block_number]
            return block_timestamp
        elif transaction:
            return transaction.get("timestamp", int(datetime.now().timestamp()))
        else:
            raise ValueError("No event or transaction provided")

    def get_max_cap(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the max cap for price calculation. Override in subclasses if needed.
        Default implementation returns 0 (no cap).
        """
        return 0

    def get_price_components(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Get price from an event.

        Args:
            event: Event dictionary

        Returns:
            Tuple of (asset, asset_source, price, timestamp)
        """
        return {
            "asset": decode_any(self.asset),
            "asset_source": decode_any(self.asset_source),
            "name": self.name,
            "timestamp": self.get_timestamp(event=event, transaction=transaction),
            "numerator": self.zero_if_negative(
                self.get_numerator(event=event, transaction=transaction)
            ),
            "denominator": self.zero_if_negative(
                self.get_denominator(event=event, transaction=transaction)
            ),
            "multiplier": self.zero_if_negative(
                self.get_multiplier(event=event, transaction=transaction)
            ),
            "max_cap": self.zero_if_negative(
                self.get_max_cap(event=event, transaction=transaction)
            ),
        }

    def zero_if_negative(self, value: int) -> int:
        if value is None:
            raise web3.exceptions.BadFunctionCallOutput("Value is None")
        return 0 if value < 0 else value

    def _get_cached_property(
        self,
        property_name: str,
        function_name: str = None,
        expiry: int = None,
        block_number: Optional[int] = None,
    ):
        """Helper method to get cached contract properties with consistent caching logic."""
        if function_name is None:
            function_name = property_name
        cache_key = self.local_cache_key(property_name)
        value = cache.get(cache_key)

        if value is None:
            value = self.call_function(function_name, block_number=block_number)
            if expiry is None:
                cache.set(cache_key, value)
            else:
                cache.set(cache_key, value, expiry)
        return value
