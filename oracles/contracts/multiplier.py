from oracles.contracts.multiplier_abis import METHOD_ABI_MAPPING
from oracles.contracts.utils import (
    CACHE_MULTIPLERS_BEFORE_BLOCK,
    AssetSourceType,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
    get_timestamp,
)
from utils.encoding import decode_any
from utils.rpc import get_evm_block_timestamps


def get_multiplier(asset: str, asset_source: str, event=None, transaction=None) -> int:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    if transaction or (event is None and transaction is None):
        return RpcCacheStorage.get_cache(asset_source, "MULTIPLIER") or 1

    # Define configuration for different asset source types
    MULTIPLIER_CONFIGS = {
        # PriceCapAdapter variants: Use ratio from ratio provider
        AssetSourceType.PriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getRatio",
            "requires_parameter": False,
        },
        AssetSourceType.OsETHPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "convertToAssets",
            "requires_parameter": True,
        },
        AssetSourceType.WstETHPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getPooledEthByShares",
            "requires_parameter": True,
        },
        AssetSourceType.SUSDePriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "convertToAssets",
            "requires_parameter": True,
        },
        AssetSourceType.RsETHPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "rsETHPrice",
            "requires_parameter": False,
        },
        AssetSourceType.RETHPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getExchangeRate",
            "requires_parameter": False,
        },
        AssetSourceType.CbETHPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "exchangeRate",
            "requires_parameter": False,
        },
        AssetSourceType.WeETHPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getRate",
            "requires_parameter": False,
        },
        AssetSourceType.EthXPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getExchangeRate",
            "requires_parameter": False,
        },
        AssetSourceType.EBTCPriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "getRate",
            "requires_parameter": False,
        },
        AssetSourceType.EUSDePriceCapAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATIO_PROVIDER",
            "decimals_key": "RATIO_DECIMALS",
            "method": "convertToAssets",
            "requires_parameter": True,
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
        AssetSourceType.sDAISynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "RATE_PROVIDER",
            "method": "chi",
            "requires_parameter": False,
        },
        AssetSourceType.CLrETHSynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "RETH",
            "method": "getExchangeRate",
            "requires_parameter": False,
        },
        AssetSourceType.CLwstETHSynchronicityPriceAdapter: {
            "type": "ratio_provider",
            "provider_key": "STETH",
            "method": "getPooledEthByShares",
            "requires_parameter": True,
        },
        # Default multiplier of 1
        AssetSourceType.EACAggregatorProxy: {"type": "default"},
        AssetSourceType.PriceCapAdapterStable: {"type": "default"},
        AssetSourceType.CLSynchronicityPriceAdapterPegToBase: {"type": "default"},
        AssetSourceType.sDAIMainnetPriceCapAdapter: {"type": "default"},
        AssetSourceType.GhoOracle: {"type": "default"},
    }

    # Get configuration for this asset source type
    config = MULTIPLIER_CONFIGS.get(asset_source_type)
    if not config:
        raise ValueError(f"Unknown asset source type: {asset_source_type}")

    config_type = config["type"]

    # Handle ratio provider type
    if config_type == "ratio_provider":
        block_number = event.blockNumber
        provider = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, config["provider_key"]
        )
        method = config["method"]
        requires_parameter = config.get("requires_parameter", False)

        # Get the appropriate ABI for this method
        abi = METHOD_ABI_MAPPING.get(method)
        if not abi:
            raise ValueError(f"No ABI found for method: {method}")

        if requires_parameter:
            decimals_key = config.get("decimals_key")
            if decimals_key:
                ratio_decimals = RpcCacheStorage.get_cached_asset_source_function(
                    asset_source, decimals_key
                )
                parameter = int(10**ratio_decimals)
                if block_number < CACHE_MULTIPLERS_BEFORE_BLOCK:
                    multiplier = RpcCacheStorage.get_cache(asset_source, "MULTIPLIER")
                    if multiplier is None:
                        multiplier = RpcCacheStorage.call_function(
                            provider,
                            method,
                            parameter,
                            block_number=block_number,
                            abi=abi,
                        )
                        RpcCacheStorage.set_cache(
                            asset_source, "MULTIPLIER", multiplier
                        )
                else:
                    multiplier = RpcCacheStorage.call_function(
                        provider, method, parameter, block_number=block_number, abi=abi
                    )
            else:
                # Handle case where parameter is needed but no decimals specified
                if block_number < CACHE_MULTIPLERS_BEFORE_BLOCK:
                    multiplier = RpcCacheStorage.get_cache(asset_source, "MULTIPLIER")
                    if multiplier is None:
                        multiplier = RpcCacheStorage.call_function(
                            provider,
                            method,
                            int(1e18),  # Default parameter as integer
                            block_number=block_number,
                            abi=abi,
                        )
                        RpcCacheStorage.set_cache(
                            asset_source, "MULTIPLIER", multiplier
                        )
                else:
                    multiplier = RpcCacheStorage.call_function(
                        provider,
                        method,
                        int(1e18),  # Default parameter as integer
                        block_number=block_number,
                        abi=abi,
                    )
        else:
            if block_number < CACHE_MULTIPLERS_BEFORE_BLOCK:
                multiplier = RpcCacheStorage.get_cache(asset_source, "MULTIPLIER")
                if multiplier is None:
                    multiplier = RpcCacheStorage.call_function(
                        provider, method, block_number=block_number, abi=abi
                    )
                    RpcCacheStorage.set_cache(asset_source, "MULTIPLIER", multiplier)
            else:
                multiplier = RpcCacheStorage.call_function(
                    provider, method, block_number=block_number, abi=abi
                )

        RpcCacheStorage.set_cache(asset_source, "MULTIPLIER", multiplier)

    # Handle Pendle discount calculation
    elif config_type == "pendle_discount":
        block_number = event.blockNumber
        block_timestamp = get_evm_block_timestamps([block_number])[block_number]

        maturity = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "MATURITY"
        )
        discount_rate_per_year = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "discountRatePerYear"
        )
        seconds_per_year = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "SECONDS_PER_YEAR"
        )
        percentage_factor = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "PERCENTAGE_FACTOR"
        )

        time_to_maturity = max(maturity - block_timestamp, 0)
        current_discount = int(
            (discount_rate_per_year * time_to_maturity) / seconds_per_year
        )
        multiplier = int(percentage_factor - current_discount)

        RpcCacheStorage.set_cache(asset_source, "MULTIPLIER", multiplier)

    # Handle default multiplier of 1
    elif config_type == "default":
        multiplier = 1

    else:
        raise UnsupportedAssetSourceError(f"Unknown config type: {config_type}")

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        multiplier,
    ]
