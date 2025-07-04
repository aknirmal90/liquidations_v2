from dataclasses import dataclass
from typing import Any, Dict, Optional

from oracles.contracts.multiplier_abis import METHOD_ABI_MAPPING
from oracles.contracts.utils import (
    AssetSourceType,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
    get_blockNumber,
    get_timestamp,
    send_unsupported_asset_source_notification,
)
from utils.encoding import decode_any
from utils.rpc import rpc_adapter

MULTIPLIER_LIVE_BLOCKS_CUTOFF = 15_000
DEFAULT_PARAMETER = int(1e18)


@dataclass
class RatioProviderConfig:
    """Configuration for ratio provider type multipliers."""

    provider_key: str
    method: str
    requires_parameter: bool = False
    decimals_key: Optional[str] = None


@dataclass
class MultiplierConfig:
    """Configuration for different multiplier types."""

    type: str
    ratio_config: Optional[RatioProviderConfig] = None


def _is_old_block(block_number: int) -> bool:
    """Check if block is old enough to use caching."""
    return (
        block_number < rpc_adapter.cached_block_height - MULTIPLIER_LIVE_BLOCKS_CUTOFF
    )


def _get_cached_or_call_function(
    asset_source: str,
    provider: str,
    method: str,
    block_number: int,
    abi: Dict[str, Any],
    cache_key: str = "MULTIPLIER",
    parameter: Optional[int] = None,
) -> int:
    """Get cached value or call function and cache result."""
    if _is_old_block(block_number):
        multiplier = RpcCacheStorage.get_cache(asset_source, cache_key)
        if multiplier is None:
            if parameter is not None:
                multiplier = RpcCacheStorage.call_function(
                    provider, method, parameter, block_number=block_number, abi=abi
                )
            else:
                multiplier = RpcCacheStorage.call_function(
                    provider, method, block_number=block_number, abi=abi
                )
            RpcCacheStorage.set_cache(asset_source, cache_key, multiplier)
    else:
        if parameter is not None:
            multiplier = RpcCacheStorage.call_function(
                provider, method, parameter, block_number=block_number, abi=abi
            )
        else:
            multiplier = RpcCacheStorage.call_function(
                provider, method, block_number=block_number, abi=abi
            )

    return multiplier


def _handle_ratio_provider(
    asset_source: str, config: RatioProviderConfig, block_number: int
) -> int:
    """Handle ratio provider type multiplier calculation."""
    provider = RpcCacheStorage.get_cached_asset_source_function(
        asset_source, config.provider_key
    )

    abi = METHOD_ABI_MAPPING.get(config.method)
    if not abi:
        raise ValueError(f"No ABI found for method: {config.method}")

    if config.requires_parameter:
        parameter = _get_parameter(asset_source, config, block_number)
        return _get_cached_or_call_function(
            asset_source,
            provider,
            config.method,
            block_number,
            abi,
            parameter=parameter,
        )
    else:
        return _get_cached_or_call_function(
            asset_source, provider, config.method, block_number, abi
        )


def _get_parameter(
    asset_source: str, config: RatioProviderConfig, block_number: int
) -> int:
    """Get parameter for ratio provider function call."""
    if config.decimals_key:
        ratio_decimals = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, config.decimals_key
        )
        return int(10**ratio_decimals)
    else:
        return DEFAULT_PARAMETER


def _handle_pendle_discount(asset_source: str, block_number: int) -> int:
    """Handle Pendle discount calculation."""
    percentage_factor = RpcCacheStorage.get_cached_asset_source_function(
        asset_source, "PERCENTAGE_FACTOR"
    )

    current_discount = _get_cached_or_call_function(
        asset_source,
        asset_source,
        "getCurrentDiscount",
        block_number,
        {},
        cache_key="CURRENT_DISCOUNT",
    )

    return int(percentage_factor - current_discount)


def _get_multiplier_configs() -> Dict[AssetSourceType, MultiplierConfig]:
    """Get multiplier configurations for all asset source types."""
    # Common ratio provider configurations
    ratio_provider_configs = {
        AssetSourceType.PriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="getRatio",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.OsETHPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="convertToAssets",
            requires_parameter=True,
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.WstETHPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="getPooledEthByShares",
            requires_parameter=True,
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.SUSDePriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="convertToAssets",
            requires_parameter=True,
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.RsETHPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="rsETHPrice",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.RETHPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="getExchangeRate",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.CbETHPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="exchangeRate",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.WeETHPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="getRate",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.EthXPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="getExchangeRate",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.EBTCPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="getRate",
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.EUSDePriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER",
            method="convertToAssets",
            requires_parameter=True,
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.WstETHSynchronicityPriceAdapter: RatioProviderConfig(
            provider_key="STETH",
            method="getPooledEthByShares",
            requires_parameter=True,
            decimals_key="RATIO_DECIMALS",
        ),
        AssetSourceType.sDAIMainnetPriceCapAdapter: RatioProviderConfig(
            provider_key="RATIO_PROVIDER", method="chi"
        ),
        AssetSourceType.sDAISynchronicityPriceAdapter: RatioProviderConfig(
            provider_key="RATE_PROVIDER", method="chi"
        ),
        AssetSourceType.CLrETHSynchronicityPriceAdapter: RatioProviderConfig(
            provider_key="RETH", method="getExchangeRate"
        ),
        AssetSourceType.CLwstETHSynchronicityPriceAdapter: RatioProviderConfig(
            provider_key="STETH", method="getPooledEthByShares", requires_parameter=True
        ),
    }

    configs = {}

    # Add ratio provider configs
    for asset_type, ratio_config in ratio_provider_configs.items():
        configs[asset_type] = MultiplierConfig(
            type="ratio_provider", ratio_config=ratio_config
        )

    # Add special configs
    configs.update(
        {
            AssetSourceType.PendlePriceCapAdapter: MultiplierConfig(
                type="pendle_discount"
            ),
            AssetSourceType.EACAggregatorProxy: MultiplierConfig(type="default"),
            AssetSourceType.PriceCapAdapterStable: MultiplierConfig(type="default"),
            AssetSourceType.CLSynchronicityPriceAdapterPegToBase: MultiplierConfig(
                type="default"
            ),
            AssetSourceType.GhoOracle: MultiplierConfig(type="default"),
            AssetSourceType.EURPriceCapAdapterStable: MultiplierConfig(type="default"),
        }
    )

    return configs


def _calculate_multiplier(
    asset_source: str,
    asset_source_type: AssetSourceType,
    config: MultiplierConfig,
    block_number: int,
) -> int:
    """Calculate multiplier based on configuration type."""
    if config.type == "ratio_provider":
        if not config.ratio_config:
            raise ValueError("Ratio provider config missing ratio_config")
        multiplier = _handle_ratio_provider(
            asset_source, config.ratio_config, block_number
        )
    elif config.type == "pendle_discount":
        multiplier = _handle_pendle_discount(asset_source, block_number)
    elif config.type == "default":
        multiplier = 1
    else:
        raise UnsupportedAssetSourceError(f"Unknown config type: {config.type}")

    RpcCacheStorage.set_cache(asset_source, "MULTIPLIER", multiplier)
    return multiplier


def get_multiplier(asset: str, asset_source: str, event=None, transaction=None) -> list:
    """Get multiplier for an asset source."""
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    # Early return for transaction or no event
    if transaction or (event is None and transaction is None):
        return RpcCacheStorage.get_cache(asset_source, "MULTIPLIER") or 1

    # Get configuration
    configs = _get_multiplier_configs()
    config = configs.get(asset_source_type)

    if not config:
        send_unsupported_asset_source_notification(
            asset_source, f"Unsupported in Multiplier {asset_source_type}"
        )
        raise ValueError(
            f"Unknown asset source type: {asset_source_type} - {asset_source}"
        )

    # Calculate multiplier
    block_number = event.blockNumber
    multiplier = _calculate_multiplier(
        asset_source, asset_source_type, config, block_number
    )

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        get_blockNumber(event, transaction),
        multiplier,
    ]
