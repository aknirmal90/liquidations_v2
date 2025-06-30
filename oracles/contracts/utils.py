import logging
from datetime import datetime
from typing import Any, Optional, Union

import requests
from django.core.cache import cache
from web3 import Web3

from utils.constants import NETWORK_NAME, PROTOCOL_NAME
from utils.rpc import get_evm_block_timestamps, rpc_adapter

logger = logging.getLogger(__name__)


CACHE_TTL_4_HOURS = 60 * 60 * 4

CACHE_TTL_24_HOURS = 60 * 60 * 24


class UnsupportedAssetSourceError(Exception):
    pass


def get_timestamp(event=None, transaction=None) -> int:
    if event:
        block = event.blockNumber
        timestamp = get_evm_block_timestamps([block])[block]
    elif transaction:
        block = transaction["epoch_and_round"]
        timestamp = get_evm_block_timestamps([block])[block]
    else:
        timestamp = int(datetime.now().timestamp() * 1_000_000)
    return timestamp


class RpcCacheStorage:
    @classmethod
    def get_cache(cls, asset_source: str, function_name: str) -> Any:
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"
        return cache.get(cache_key)

    @classmethod
    def set_cache(
        cls, asset_source: str, function_name: str, value: Union[str, int]
    ) -> None:
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"
        cache.set(cache_key, value)

    @classmethod
    def set_cache_with_ttl(
        cls, asset_source: str, function_name: str, value: Union[str, int], ttl: int
    ) -> None:
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"
        cache.set(cache_key, value, ttl)

    @classmethod
    def get_cached_asset_source_function(
        cls,
        asset_source: str,
        function_name: str,
        ttl: Optional[int] = None,
        *args,
        **kwargs,
    ) -> Any:
        cache_key = f"{NETWORK_NAME}:{PROTOCOL_NAME}:{asset_source}:{function_name}"
        if cache.get(cache_key):
            return cache.get(cache_key)
        else:
            value = cls.call_function(asset_source, function_name, *args, **kwargs)
            if ttl:
                cls.set_cache_with_ttl(asset_source, function_name, value, ttl)
            else:
                cls.set_cache(asset_source, function_name, value)
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
        name = cache.get(cache_name_key)
        abi = cache.get(cache_abi_key)

        if name and abi:
            return name, abi

        response = requests.get(
            f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={asset_source}&apikey=KNVMN27EBAAE1E9MUMX75N2QKFZWB2HB6J"
        )
        response = response.json()
        result = response["result"][0]
        if isinstance(result, str):
            logger.info(f"ABI is a string for {asset_source}")
        name = result["ContractName"]
        abi = result["ABI"]
        cls.set_cache(asset_source, "ASSET_SOURCE_NAME", name)
        cls.set_cache(asset_source, "ASSET_SOURCE_ABI", abi)
        return name, abi


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
