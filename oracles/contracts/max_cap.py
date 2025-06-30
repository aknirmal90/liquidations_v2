from web3 import Web3

from oracles.contracts.utils import (
    CACHE_TTL_4_HOURS,
    AssetSourceType,
    RpcCacheStorage,
    UnsupportedAssetSourceError,
    get_timestamp,
)
from utils.encoding import decode_any
from utils.rpc import get_evm_block_timestamps, rpc_adapter


def get_max_cap(asset: str, asset_source: str, event=None, transaction=None) -> int:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    if transaction or (event is None and transaction is None):
        return RpcCacheStorage.get_cache(asset_source, "MAX_CAP") or 0

    # PriceCapAdapterStable: Get MAX_CAP from event data
    if asset_source_type == AssetSourceType.PriceCapAdapterStable:
        events = rpc_adapter.extract_raw_event_data(
            topics=[
                "0xa89f50d1caf6c404765ce94b422be388ce69c8ed68921620fa6a83c810000615"
            ],
            contract_addresses=[Web3.to_checksum_address(asset_source)],
            start_block=0,
            end_block=rpc_adapter.block_height,
        )
        if events:
            latest_event = events[-1]
            data = decode_any(latest_event.data)
            max_cap = int(data, 16)
            RpcCacheStorage.set_cache_with_ttl(
                asset_source, "MAX_CAP", max_cap, CACHE_TTL_4_HOURS
            )
        else:
            raise ValueError(
                f"No event found for {asset_source} of type {asset_source_type}"
            )

    # PriceCapAdapter: Calculate max cap based on snapshot ratio and growth rate
    elif asset_source_type in (
        AssetSourceType.OsETHPriceCapAdapter,
        AssetSourceType.WstETHPriceCapAdapter,
        AssetSourceType.SUSDePriceCapAdapter,
        AssetSourceType.RsETHPriceCapAdapter,
        AssetSourceType.RETHPriceCapAdapter,
        AssetSourceType.CbETHPriceCapAdapter,
        AssetSourceType.WeETHPriceCapAdapter,
        AssetSourceType.EthXPriceCapAdapter,
        AssetSourceType.EBTCPriceCapAdapter,
        AssetSourceType.EUSDePriceCapAdapter,
        AssetSourceType.sDAIMainnetPriceCapAdapter,
    ):
        block_number = event.blockNumber
        block_timestamp = get_evm_block_timestamps([block_number])[block_number]

        snapshot_ratio = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "getSnapshotRatio", ttl=CACHE_TTL_4_HOURS
        )
        max_ratio_growth_per_second = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "getMaxRatioGrowthPerSecond", ttl=CACHE_TTL_4_HOURS
        )
        snapshot_timestamp = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "getSnapshotTimestamp", ttl=CACHE_TTL_4_HOURS
        )

        max_cap = snapshot_ratio + max_ratio_growth_per_second * (
            block_timestamp - snapshot_timestamp
        )
        RpcCacheStorage.set_cache_with_ttl(
            asset_source, "MAX_CAP", max_cap, CACHE_TTL_4_HOURS
        )

    # All other asset source types: No max cap (return 0)
    elif asset_source_type in [
        AssetSourceType.EACAggregatorProxy,
        AssetSourceType.PendlePriceCapAdapter,
        AssetSourceType.CLSynchronicityPriceAdapterPegToBase,
        AssetSourceType.CLrETHSynchronicityPriceAdapter,
        AssetSourceType.CLrETHSynchronicityPriceAdapterPegToBase,
        AssetSourceType.CLwstETHSynchronicityPriceAdapter,
        AssetSourceType.GhoOracle,
        AssetSourceType.WstETHSynchronicityPriceAdapter,
        AssetSourceType.sDAISynchronicityPriceAdapter,
    ]:
        max_cap = 0
    else:
        raise UnsupportedAssetSourceError(
            f"Unknown asset source type: {asset_source_type}"
        )

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        max_cap,
    ]
