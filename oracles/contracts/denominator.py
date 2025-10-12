from oracles.contracts.utils import (
    CACHE_TTL_4_HOURS,
    AssetSourceType,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
    get_blockNumber,
    get_timestamp,
    send_unsupported_asset_source_notification,
)
from utils.encoding import decode_any


def get_denominator(asset: str, asset_source: str, event=None, transaction=None) -> int:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    if asset_source_type in [
        AssetSourceType.CLrETHSynchronicityPriceAdapter,
        AssetSourceType.sDAIMainnetPriceCapAdapter,
        AssetSourceType.WstETHPriceCapAdapter,
        AssetSourceType.WeETHPriceCapAdapter,
        AssetSourceType.SUSDePriceCapAdapter,
        AssetSourceType.RsETHPriceCapAdapter,
        AssetSourceType.RETHPriceCapAdapter,
        AssetSourceType.OsETHPriceCapAdapter,
        AssetSourceType.CbETHPriceCapAdapter,
        AssetSourceType.EthXPriceCapAdapter,
        AssetSourceType.EBTCPriceCapAdapter,
        AssetSourceType.EUSDePriceCapAdapter,
        AssetSourceType.sDAISynchronicityPriceAdapter,
        AssetSourceType.WstETHSynchronicityPriceAdapter,
        AssetSourceType.CLrETHSynchronicityPriceAdapterPegToBase,
        AssetSourceType.EzETHPriceCapAdapter,
        AssetSourceType.LBTCPriceCapAdapter,
    ]:
        denominator = 10 ** RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "RATIO_DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
    elif asset_source_type == AssetSourceType.CLwstETHSynchronicityPriceAdapter:
        decimals = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
        denominator_base = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DENOMINATOR", ttl=CACHE_TTL_4_HOURS
        ) * 10 ** RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "RATIO_DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
        denominator = int(denominator_base / (10**decimals))
    elif asset_source_type == AssetSourceType.PendlePriceCapAdapter:
        denominator = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "PERCENTAGE_FACTOR", ttl=CACHE_TTL_4_HOURS
        )
    elif asset_source_type in (AssetSourceType.CLSynchronicityPriceAdapterPegToBase,):
        # Get decimals for price calculation
        decimals = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
        denominator_base = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DENOMINATOR", ttl=CACHE_TTL_4_HOURS
        )
        denominator = int(denominator_base / (10**decimals))

    elif asset_source_type == AssetSourceType.TETHPriceCapAdapter:
        denominator_1 = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "RATIO_DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
        underlying = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "BASE_TO_USD_AGGREGATOR", ttl=CACHE_TTL_4_HOURS
        )
        denominator_2 = RpcCacheStorage.get_cached_asset_source_function(
            underlying, "RATIO_DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
        denominator = (10**denominator_2) * (10**denominator_1)
    elif asset_source_type in [
        AssetSourceType.GhoOracle,
        AssetSourceType.EACAggregatorProxy,
        AssetSourceType.PriceCapAdapterStable,
        AssetSourceType.EURPriceCapAdapterStable,
    ]:
        denominator = 1
    else:
        send_unsupported_asset_source_notification(
            asset_source, f"Unsupported in Price Denominator {asset_source_type}"
        )
        raise UnsupportedAssetSourceError(
            f"Unknown asset source type: {asset_source_type} - {asset_source}"
        )

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        get_blockNumber(event, transaction),
        denominator,
    ]
