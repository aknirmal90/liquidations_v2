from typing import List

from oracles.contracts.utils import (
    AssetSourceType,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
)
from utils.encoding import decode_any


def get_underlying_sources(asset_source: str) -> List[str]:
    """
    Get the most underlying asset sources for a given asset_source address.
    Recursively resolves all underlying sources until only EACAggregatorProxy types remain.

    Args:
        asset_source: The asset source address to analyze

    Returns:
        List of underlying asset source addresses that should be monitored
    """
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    # If this is already an EACAggregatorProxy, return it as it's the most underlying source
    if asset_source_type == AssetSourceType.EACAggregatorProxy:
        aggregator = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "aggregator"
        )
        return [decode_any(aggregator)]

    # Define mapping of asset source types to their underlying source configurations
    UNDERLYING_SOURCE_CONFIGS = {
        # PriceCapAdapter variants: Single BASE_TO_USD_AGGREGATOR source
        AssetSourceType.PriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.OsETHPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.WstETHPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.SUSDePriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.RsETHPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.RETHPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.CbETHPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.WeETHPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.EthXPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.EBTCPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.EUSDePriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        AssetSourceType.sDAIMainnetPriceCapAdapter: ["BASE_TO_USD_AGGREGATOR"],
        # PendlePriceCapAdapter variants: Single ASSET_TO_USD_AGGREGATOR source
        AssetSourceType.PendlePriceCapAdapter: ["ASSET_TO_USD_AGGREGATOR"],
        AssetSourceType.PriceCapAdapterStable: ["ASSET_TO_USD_AGGREGATOR"],
        # Single source adapters
        AssetSourceType.sDAISynchronicityPriceAdapter: ["DAI_TO_USD"],
        AssetSourceType.WstETHSynchronicityPriceAdapter: ["ETH_TO_BASE"],
        AssetSourceType.CLrETHSynchronicityPriceAdapter: ["ETH_TO_USD"],
        # Dual source adapters
        AssetSourceType.CLSynchronicityPriceAdapterPegToBase: [
            "ASSET_TO_PEG",
            "PEG_TO_BASE",
        ],
        AssetSourceType.CLwstETHSynchronicityPriceAdapter: [
            "ASSET_TO_PEG",
            "PEG_TO_BASE",
        ],
    }

    # Check if asset source type has a configuration
    if asset_source_type in UNDERLYING_SOURCE_CONFIGS:
        source_names = UNDERLYING_SOURCE_CONFIGS[asset_source_type]

        # Handle single source
        if len(source_names) == 1:
            source_address = RpcCacheStorage.get_cached_asset_source_function(
                asset_source, source_names[0]
            )
            return get_underlying_sources(source_address)

        # Handle multiple sources
        else:
            underlying_sources = []
            for source_name in source_names:
                source_address = RpcCacheStorage.get_cached_asset_source_function(
                    asset_source, source_name
                )
                underlying_sources.extend(get_underlying_sources(source_address))
            return underlying_sources

    # GhoOracle: No underlying sources, fixed price
    elif asset_source_type == AssetSourceType.GhoOracle:
        return ["0xd110cac5d8682a3b045d5524a9903e031d70fccd"]

    # Unknown asset source type
    else:
        raise UnsupportedAssetSourceError(
            f"Unknown asset source type: {asset_source_type} - {asset_source}"
        )
