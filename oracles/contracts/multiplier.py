from oracles.contracts.multiplier_abis import METHOD_ABI_MAPPING
from oracles.contracts.utils import (
    CACHE_TTL_1_MINUTE,
    CACHE_TTL_4_HOURS,
    AssetSourceType,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
    get_blockNumber,
    get_timestamp,
    send_unsupported_asset_source_notification,
)
from utils.encoding import decode_any


def _calculate_pendle_discount_multiplier(asset_source: str, event) -> int:
    """Calculate multiplier for pendle discount asset source types."""
    percentage_factor = RpcCacheStorage.get_cached_asset_source_function(
        asset_source, "PERCENTAGE_FACTOR"
    )
    current_discount = RpcCacheStorage.get_cache(asset_source, "CURRENT_DISCOUNT")
    if current_discount is None:
        current_discount = RpcCacheStorage.call_function(
            asset_source, "getCurrentDiscount"
        )
        RpcCacheStorage.set_cache_with_ttl(
            asset_source, "CURRENT_DISCOUNT", current_discount, ttl=CACHE_TTL_1_MINUTE
        )

    multiplier = int(percentage_factor - current_discount)
    RpcCacheStorage.set_cache_with_ttl(
        asset_source, "MULTIPLIER", multiplier, CACHE_TTL_1_MINUTE
    )
    return multiplier


def _calculate_ratio_provider_multiplier(asset_source: str, config: dict, event) -> int:
    """Calculate multiplier for ratio provider asset source types."""
    provider = RpcCacheStorage.get_cached_asset_source_function(
        asset_source, config["provider_key"], ttl=CACHE_TTL_1_MINUTE
    )
    method = config["method"]
    requires_parameter = config.get("requires_parameter", False)

    # Get the appropriate ABI for this method
    abi = METHOD_ABI_MAPPING.get(method)
    if not abi:
        raise ValueError(f"No ABI found for method: {method}")

    if requires_parameter:
        ratio_decimals = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, config["decimals_key"], ttl=CACHE_TTL_1_MINUTE
        )
        parameter = int(10**ratio_decimals)
        multiplier = RpcCacheStorage.get_cache(asset_source, "MULTIPLIER")
        if multiplier is None:
            multiplier = RpcCacheStorage.call_function(
                provider,
                method,
                parameter,
                abi=abi,
            )
            RpcCacheStorage.set_cache_with_ttl(
                asset_source, "MULTIPLIER", multiplier, CACHE_TTL_1_MINUTE
            )
    else:
        multiplier = RpcCacheStorage.get_cache(asset_source, "MULTIPLIER")
        if multiplier is None:
            multiplier = RpcCacheStorage.get_cached_asset_source_function(
                provider, method, abi=abi, ttl=CACHE_TTL_1_MINUTE
            )
            RpcCacheStorage.set_cache_with_ttl(
                asset_source, "MULTIPLIER", multiplier, CACHE_TTL_1_MINUTE
            )

    return multiplier


def _calculate_default_multiplier() -> int:
    """Calculate multiplier for default asset source types."""
    return 1


def _calculate_static_get_ratio_multiplier(
    asset_source: str, config: dict, event
) -> int:
    """Calculate multiplier for static get ratio asset source types."""
    return RpcCacheStorage.get_cached_asset_source_function(
        asset_source, config["method"], ttl=CACHE_TTL_4_HOURS
    )


def get_multiplier(asset: str, asset_source: str, event=None, transaction=None) -> int:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    if transaction or (event is None and transaction is None):
        return RpcCacheStorage.get_cache(asset_source, "MULTIPLIER") or 1

    # Define configuration for different asset source types
    MULTIPLIER_CONFIGS = {
        # PriceCapAdapter variants: Use ratio from static get ratio
        AssetSourceType.PriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.OsETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.WstETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.SUSDePriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.RsETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.RETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.CbETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.WeETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.EthXPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.EBTCPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.EUSDePriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        # PendlePriceCapAdapter: Uses discount calculation
        AssetSourceType.PendlePriceCapAdapter: {"type": "pendle_discount"},
        # SynchronicityPriceAdapter variants
        AssetSourceType.WstETHSynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "STETH",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getPooledEthByShares",
            "requires_parameter": True,
        },
        AssetSourceType.sDAIMainnetPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.sDAISynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATE_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "chi",
            "requires_parameter": False,
        },
        AssetSourceType.CLrETHSynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "RETH",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getExchangeRate",
            "requires_parameter": False,
        },
        AssetSourceType.CLwstETHSynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "STETH",
            "method": "getPooledEthByShares",
            "decimals_key": "RATIO_DECIMALS",
            "requires_parameter": True,
        },
        # Default multiplier of 1
        AssetSourceType.EACAggregatorProxy: {"type": "default"},
        AssetSourceType.PriceCapAdapterStable: {"type": "default"},
        AssetSourceType.CLSynchronicityPriceAdapterPegToBase: {"type": "default"},
        AssetSourceType.GhoOracle: {"type": "default"},
        AssetSourceType.EURPriceCapAdapterStable: {"type": "default"},
        AssetSourceType.TETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.EzETHPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
        AssetSourceType.LBTCPriceCapAdapter: {
            "type": "static_get_ratio",
            "method": "getRatio",
        },
    }

    # Get configuration for this asset source type
    config = MULTIPLIER_CONFIGS.get(asset_source_type)
    if not config:
        send_unsupported_asset_source_notification(
            asset_source, f"Unsupported in Multiplier {asset_source_type}"
        )
        raise ValueError(
            f"Unknown asset source type: {asset_source_type} - {asset_source}"
        )

    config_type = config["type"]

    # Handle Pendle discount calculation
    if config_type == "pendle_discount":
        multiplier = _calculate_pendle_discount_multiplier(asset_source, event)

    # Handle default multiplier of 1
    elif config_type == "default":
        multiplier = _calculate_default_multiplier()

    elif config_type == "static_get_ratio":
        multiplier = _calculate_static_get_ratio_multiplier(asset_source, config, event)

    elif config_type == "ratio_provider":
        multiplier = _calculate_ratio_provider_multiplier(asset_source, config, event)

    else:
        raise UnsupportedAssetSourceError(f"Unknown config type: {config_type}")

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        get_blockNumber(event, transaction),
        multiplier,
    ]
