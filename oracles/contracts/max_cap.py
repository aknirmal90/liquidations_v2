import logging

from web3 import Web3

from oracles.contracts.denominator import get_denominator
from oracles.contracts.multiplier import get_multiplier
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
from utils.rpc import get_evm_block_timestamps, rpc_adapter

logger = logging.getLogger(__name__)


class MaxCapType:
    NO_CAP = 0
    MAX_PRICE_CAP = 1
    MAX_MULTIPLIER_CAP = 2


def get_max_cap(asset: str, asset_source: str, event=None, transaction=None) -> list:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    logger.info(f"Getting max cap for {asset_source} of type {asset_source_type}")

    if transaction or (event is None and transaction is None):
        return RpcCacheStorage.get_cache(asset_source, "MAX_CAP") or 0

    # Initialize max_cap_type: 0=no cap, 1=max price cap, 2=max ratio cap
    max_cap_type = MaxCapType.NO_CAP

    # PriceCapAdapterStable: Get MAX_CAP from event data
    if asset_source_type == AssetSourceType.PriceCapAdapterStable:
        max_cap_type = MaxCapType.MAX_PRICE_CAP
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

    elif asset_source_type == AssetSourceType.PendlePriceCapAdapter:
        max_cap_type = MaxCapType.MAX_PRICE_CAP
        asset_to_usd_aggregator = decode_any(
            RpcCacheStorage.get_cached_asset_source_function(
                asset_source, "ASSET_TO_USD_AGGREGATOR"
            )
        )
        max_cap_underlying = get_max_cap(
            asset, asset_to_usd_aggregator, event, transaction
        )[-2]
        multiplier = get_multiplier(asset, asset_source, event, transaction)[-1]
        denominator = get_denominator(asset, asset_source, event, transaction)[-1]
        max_cap = int(max_cap_underlying * multiplier / denominator)
        RpcCacheStorage.set_cache_with_ttl(
            asset_source, "MAX_CAP", max_cap, CACHE_TTL_4_HOURS
        )

    elif asset_source_type == AssetSourceType.EURPriceCapAdapterStable:
        max_cap_type = MaxCapType.MAX_PRICE_CAP
        events = rpc_adapter.extract_raw_event_data(
            topics=[
                "0x816ed2ec97505a2cbad39de6d4f0be098ab74467f5de87c86c000e64edf52c55"
            ],
            contract_addresses=[Web3.to_checksum_address(asset_source)],
            start_block=0,
            end_block=rpc_adapter.block_height,
        )
        if events:
            latest_event = events[-1]
            data = decode_any(latest_event.data)
            max_cap_event = int(data, 16)
            BASE_TO_USD_AGGREGATOR = RpcCacheStorage.get_cached_asset_source_function(
                asset_source, "BASE_TO_USD_AGGREGATOR"
            )
            base_price = RpcCacheStorage.get_cached_asset_source_function(
                BASE_TO_USD_AGGREGATOR, "latestAnswer", ttl=CACHE_TTL_4_HOURS
            )
            ratio_decimals = RpcCacheStorage.get_cached_asset_source_function(
                asset_source, "RATIO_DECIMALS"
            )
            max_cap = int(max_cap_event * base_price / (10**ratio_decimals))
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
        AssetSourceType.EzETHPriceCapAdapter,
        AssetSourceType.LBTCPriceCapAdapter,
    ):
        max_cap_type = MaxCapType.MAX_MULTIPLIER_CAP
        block_number = event.blockNumber
        block_timestamp = (
            get_evm_block_timestamps([block_number])[block_number] / 1_000_000
        )

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

    elif asset_source_type == AssetSourceType.TETHPriceCapAdapter:
        max_cap_type = MaxCapType.MAX_MULTIPLIER_CAP
        block_number = event.blockNumber
        block_timestamp = (
            get_evm_block_timestamps([block_number])[block_number] / 1_000_000
        )

        snapshot_ratio_1 = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "getSnapshotRatio", ttl=CACHE_TTL_4_HOURS
        )
        max_ratio_growth_per_second_1 = (
            RpcCacheStorage.get_cached_asset_source_function(
                asset_source, "getMaxRatioGrowthPerSecond", ttl=CACHE_TTL_4_HOURS
            )
        )
        snapshot_timestamp_1 = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "getSnapshotTimestamp", ttl=CACHE_TTL_4_HOURS
        )
        max_cap_1 = snapshot_ratio_1 + max_ratio_growth_per_second_1 * (
            block_timestamp - snapshot_timestamp_1
        )
        underlying = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "BASE_TO_USD_AGGREGATOR", ttl=CACHE_TTL_4_HOURS
        )
        snapshot_ratio_2 = RpcCacheStorage.get_cached_asset_source_function(
            underlying, "getSnapshotRatio", ttl=CACHE_TTL_4_HOURS
        )
        max_ratio_growth_per_second_2 = (
            RpcCacheStorage.get_cached_asset_source_function(
                underlying, "getMaxRatioGrowthPerSecond", ttl=CACHE_TTL_4_HOURS
            )
        )
        snapshot_timestamp_2 = RpcCacheStorage.get_cached_asset_source_function(
            underlying, "getSnapshotTimestamp", ttl=CACHE_TTL_4_HOURS
        )
        max_cap_2 = snapshot_ratio_2 + max_ratio_growth_per_second_2 * (
            block_timestamp - snapshot_timestamp_2
        )
        max_cap = max_cap_1 * max_cap_2
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
        max_cap_type = MaxCapType.NO_CAP
    else:
        send_unsupported_asset_source_notification(
            asset_source, f"Unsupported in Max Cap {asset_source_type}"
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
        max_cap,
        max_cap_type,
    ]
