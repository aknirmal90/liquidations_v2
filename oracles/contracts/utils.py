import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Optional, Union

import requests
from django.core.cache import cache
from django.db.models import QuerySet
from web3 import Web3

from oracles.models import PriceEvent
from utils.constants import NETWORK_NAME, PROTOCOL_NAME
from utils.encoding import decode_any
from utils.rpc import get_evm_block_timestamps, rpc_adapter
from utils.simplepush import send_simplepush_notification

logger = logging.getLogger(__name__)


CACHE_TTL_1_MINUTE = 60

CACHE_TTL_4_HOURS = 60 * 60 * 4

CACHE_TTL_24_HOURS = 60 * 60 * 24


# Simple in-memory cache using Python's built-in dict
# Much simpler than custom implementation, and we don't need complex TTL logic
# since Redis handles the primary TTL, and memory cache is just for speed
_memory_cache = {}
_memory_cache_lock = __import__("threading").Lock()


def _get_from_memory_cache(cache_key: str) -> Optional[Any]:
    """Get value from simple memory cache."""
    with _memory_cache_lock:
        return _memory_cache.get(cache_key)


def _set_memory_cache(cache_key: str, value: Any) -> None:
    """Set value in simple memory cache."""
    with _memory_cache_lock:
        _memory_cache[cache_key] = value


def _clear_memory_cache() -> None:
    """Clear the memory cache."""
    with _memory_cache_lock:
        _memory_cache.clear()


class UnsupportedAssetSourceError(Exception):
    pass


def get_timestamp(event=None, transaction=None) -> int:
    if event:
        block = event.blockNumber
        timestamp = get_evm_block_timestamps([block])[block]
    else:
        timestamp = int(datetime.now().timestamp() * 1_000_000)
    return timestamp


def get_blockNumber(event=None, transaction=None) -> int:
    if event:
        return event.blockNumber
    else:
        return rpc_adapter.cached_block_height


def get_transaction_hash(event=None, transaction=None) -> str:
    if event:
        return event.transactionHash
    else:
        return transaction["hash"]


def get_latest_asset_sources() -> QuerySet:
    return PriceEvent.objects.filter(is_active=True)


class RpcCacheStorage:
    @classmethod
    def get_cache(cls, asset_source: str, function_name: str) -> Any:
        """
        Multi-level cache retrieval:
        1. Check in-memory cache first (fastest)
        2. Check Redis cache (medium speed)
        3. Return None if not found (will trigger contract call)
        """
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"

        # Level 1: Check in-memory cache
        value = _get_from_memory_cache(cache_key)
        if value is not None:
            return value

        # Level 2: Check Redis cache
        value = cache.get(cache_key)
        if value is not None:
            # Store in memory cache for future requests
            _set_memory_cache(cache_key, value)
            return value

        return None

    @classmethod
    def set_cache(
        cls, asset_source: str, function_name: str, value: Union[str, int]
    ) -> None:
        """
        Store value in both in-memory cache and Redis cache.
        """
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"

        # Store in Redis (persistent cache)
        cache.set(cache_key, value)

        # Store in memory cache
        _set_memory_cache(cache_key, value)

    @classmethod
    def set_cache_with_ttl(
        cls, asset_source: str, function_name: str, value: Union[str, int], ttl: int
    ) -> None:
        """
        Store value in both in-memory cache and Redis cache with custom TTL.
        """
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"

        # Store in Redis with custom TTL
        cache.set(cache_key, value, ttl)

        # Store in memory cache (no TTL needed - Redis handles expiration)
        _set_memory_cache(cache_key, value)

    @classmethod
    def get_cached_asset_source_function(
        cls,
        asset_source: str,
        function_name: str,
        ttl: Optional[int] = None,
        *args,
        **kwargs,
    ) -> Any:
        """
        Multi-level cached function call:
        1. Check in-memory cache (fastest)
        2. Check Redis cache (medium speed)
        3. Call contract function (slowest)
        4. Store result in both caches
        """
        # Try to get from cache (checks memory first, then Redis)
        cached_value = cls.get_cache(asset_source, function_name)
        if cached_value is not None:
            return cached_value

        # Cache miss - call the contract function
        value = cls.call_function(asset_source, function_name, *args, **kwargs)
        decoded_value = decode_any(value)

        # Store in both caches
        if ttl:
            cls.set_cache_with_ttl(asset_source, function_name, decoded_value, ttl)
        else:
            cls.set_cache(asset_source, function_name, decoded_value)

        return value

    @classmethod
    def call_function(
        cls,
        asset_source: str,
        function_name: str,
        *args,
        block_number: Optional[int] = None,
        abi=None,
        **kwargs,
    ) -> Any:
        if abi is None:
            name, abi = cls.get_contract_info(asset_source)

        contract = rpc_adapter.client.eth.contract(
            address=Web3.to_checksum_address(asset_source), abi=abi
        )

        logger.info(
            f"Calling function {function_name} with args {args} and kwargs {kwargs}"
            + (f" at block {block_number}" if block_number is not None else "")
        )

        # Get the function and prepare the call
        func = contract.functions[function_name](*args, **kwargs)

        # Call with block number if specified, otherwise call normally
        if block_number is not None:
            return func.call(block_identifier=block_number)
        else:
            return func.call()

    @classmethod
    def get_contract_info(cls, asset_source: str) -> tuple[str, str]:
        cache_name_key = (
            f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:ASSET_SOURCE_NAME"
        )
        cache_abi_key = (
            f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:ASSET_SOURCE_ABI"
        )

        # First attempt: Check multi-level cache (memory + Redis)
        name = _get_from_memory_cache(cache_name_key)
        abi = _get_from_memory_cache(cache_abi_key)

        if name and abi:
            return name, abi

        # Second attempt: Check Redis cache
        name = cache.get(cache_name_key)
        abi = cache.get(cache_abi_key)

        if name and abi:
            # Store in memory cache for future requests
            _set_memory_cache(cache_name_key, name)
            _set_memory_cache(cache_abi_key, abi)
            return name, abi

        # Second attempt: Check file system
        abi_file_path = os.path.join(
            os.path.dirname(__file__), "abis", f"{asset_source.lower()}.json"
        )

        if os.path.exists(abi_file_path):
            try:
                with open(abi_file_path, "r") as f:
                    abi_data = json.load(f)
                    name = abi_data.get("name", "")
                    abi = abi_data.get("abi", "")

                    if name and abi:
                        # Save to both caches for future use
                        cls.set_cache(asset_source, "ASSET_SOURCE_NAME", name)
                        cls.set_cache(asset_source, "ASSET_SOURCE_ABI", abi)
                        return name, abi
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load ABI from file {abi_file_path}: {e}")

        # Third attempt: Fetch from Etherscan
        response = requests.get(
            f"https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getsourcecode&address={asset_source}&apikey=HRZ5P3FVMN1FEZUDVWUI6CFPZYZ6XJK1CN"
        )

        response = response.json()
        result = response["result"][0]
        if isinstance(result, str):
            logger.info(f"ABI is a string for {asset_source}")
            logger.info(f"Result: {response['result']}")

        try:
            name = result["ContractName"]
            abi = result["ABI"]
        except TypeError:
            logger.warning(f"ABI is not a string for {result}")
            return None, None

        # Save to both caches
        cls.set_cache(asset_source, "ASSET_SOURCE_NAME", name)
        cls.set_cache(asset_source, "ASSET_SOURCE_ABI", abi)

        # Save to file
        try:
            os.makedirs(os.path.dirname(abi_file_path), exist_ok=True)
            with open(abi_file_path, "w") as f:
                json.dump({"name": name, "abi": abi}, f, indent=2)
        except IOError as e:
            logger.warning(f"Failed to save ABI to file {abi_file_path}: {e}")

        time.sleep(1)
        return name, abi

    @classmethod
    def clear_memory_cache(cls) -> None:
        """Clear the in-memory cache. Useful for testing or memory management."""
        _clear_memory_cache()


class AssetSourceType:
    EACAggregatorProxy = "EACAggregatorProxy"
    PriceCapAdapterStable = "PriceCapAdapterStable"
    PendlePriceCapAdapter = "PendlePriceCapAdapter"
    CLSynchronicityPriceAdapterPegToBase = "CLSynchronicityPriceAdapterPegToBase"
    CLrETHSynchronicityPriceAdapter = "CLrETHSynchronicityPriceAdapter"
    CLrETHSynchronicityPriceAdapterPegToBase = (
        "CLrETHSynchronicityPriceAdapterPegToBase"
    )
    CLwstETHSynchronicityPriceAdapter = "CLwstETHSynchronicityPriceAdapter"
    sDAIMainnetPriceCapAdapter = "sDAIMainnetPriceCapAdapter"
    PriceCapAdapter = "PriceCapAdapter"
    WstETHPriceCapAdapter = "WstETHPriceCapAdapter"
    WeETHPriceCapAdapter = "WeETHPriceCapAdapter"
    SUSDePriceCapAdapter = "SUSDePriceCapAdapter"
    RsETHPriceCapAdapter = "RsETHPriceCapAdapter"
    RETHPriceCapAdapter = "RETHPriceCapAdapter"
    OsETHPriceCapAdapter = "OsETHPriceCapAdapter"
    CbETHPriceCapAdapter = "CbETHPriceCapAdapter"
    GhoOracle = "GhoOracle"
    EthXPriceCapAdapter = "EthXPriceCapAdapter"
    EBTCPriceCapAdapter = "EBTCPriceCapAdapter"
    EUSDePriceCapAdapter = "EUSDePriceCapAdapter"
    WstETHSynchronicityPriceAdapter = "WstETHSynchronicityPriceAdapter"
    sDAISynchronicityPriceAdapter = "sDAISynchronicityPriceAdapter"
    EURPriceCapAdapterStable = "EURPriceCapAdapterStable"
    TETHPriceCapAdapter = "TETHPriceCapAdapter"
    EzETHPriceCapAdapter = "EzETHPriceCapAdapter"
    LBTCPriceCapAdapter = "LBTCPriceCapAdapter"


def send_unsupported_asset_source_notification(asset_source: str, event: str):
    send_simplepush_notification(
        title="Unsupported Asset Source",
        message=f"Unsupported asset source: {asset_source}",
        event=event,
    )
