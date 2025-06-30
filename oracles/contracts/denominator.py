from oracles.contracts.utils import CACHE_TTL_4_HOURS, AssetSourceType, RpcCacheStorage
from utils.encoding import decode_any


def get_denominator(asset_source: str, event=None, transaction=None) -> int:
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
    ]:
        return 10 ** RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "RATIO_DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
    elif asset_source_type == AssetSourceType.CLSynchronicityPriceAdapterPegToBase:
        return RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DENOMINATOR", ttl=CACHE_TTL_4_HOURS
        )
    elif asset_source_type == AssetSourceType.CLwstETHSynchronicityPriceAdapter:
        return RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DENOMINATOR", ttl=CACHE_TTL_4_HOURS
        ) * 10 ** RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "RATIO_DECIMALS", ttl=CACHE_TTL_4_HOURS
        )
    elif asset_source_type == AssetSourceType.PendlePriceCapAdapter:
        return RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "PERCENTAGE_FACTOR", ttl=CACHE_TTL_4_HOURS
        )
    elif asset_source_type in [
        AssetSourceType.GhoOracle,
        AssetSourceType.EACAggregatorProxy,
        AssetSourceType.PriceCapAdapterStable,
    ]:
        return 1
    else:
        raise ValueError(f"Unknown asset source type: {asset_source_type}")
