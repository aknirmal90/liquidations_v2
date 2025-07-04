from oracles.contracts.utils import (
    AssetSourceType,
    RpcCacheStorage,
    get_blockNumber,
    get_timestamp,
    send_unsupported_asset_source_notification,
)
from utils.constants import NETWORK_BLOCK_TIME
from utils.encoding import decode_any


def get_numerator(asset: str, asset_source: str, event=None, transaction=None) -> int:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    # Handle Chainlink Synchronicity Price Adapters
    if asset_source_type in (
        AssetSourceType.CLSynchronicityPriceAdapterPegToBase,
        AssetSourceType.CLwstETHSynchronicityPriceAdapter,
    ):
        # Extract price and address from event or transaction
        if event:
            this_price = int(event.args.answer)
            address = decode_any(event.address)
        elif transaction:
            this_price = int(transaction["median_price"])
            address = decode_any(transaction["oracle_address"])

        # Determine if this is the asset-to-peg or peg-to-base price
        asset_to_peg_address = decode_any(
            RpcCacheStorage.get_cached_asset_source_function(
                asset_source, "ASSET_TO_PEG"
            )
        )
        underlying_asset_address = RpcCacheStorage.get_cached_asset_source_function(
            asset_to_peg_address, "aggregator"
        )
        is_asset_to_peg = address == underlying_asset_address

        # Get cached prices based on address type
        if is_asset_to_peg:
            other_price_past = RpcCacheStorage.get_cache(
                asset_source, "PEG_TO_BASE_PRICE"
            )
            other_price_future = RpcCacheStorage.get_cache(
                asset_source, "PEG_TO_BASE_PRICE_FUTURE"
            )
        else:
            other_price_past = RpcCacheStorage.get_cache(
                asset_source, "ASSET_TO_PEG_PRICE"
            )
            other_price_future = RpcCacheStorage.get_cache(
                asset_source, "ASSET_TO_PEG_PRICE_FUTURE"
            )

        # Use 0 if no cached past price available
        if other_price_past is None:
            other_price_past = 0

        # Handle event-based price updates
        if event:
            price = this_price * other_price_past

            # Cache the calculated price
            if is_asset_to_peg:
                if this_price and this_price > 0:
                    RpcCacheStorage.set_cache(
                        asset_source, "ASSET_TO_PEG_PRICE", this_price
                    )
            else:
                if this_price and this_price > 0:
                    RpcCacheStorage.set_cache(
                        asset_source, "PEG_TO_BASE_PRICE", this_price
                    )

        # Handle transaction-based price updates
        elif transaction:
            # Use future price if available, otherwise fall back to past price
            multiplier_price = (
                other_price_future if other_price_future else other_price_past
            )
            price = this_price * multiplier_price

            # Cache the calculated price with TTL for future transactions
            if is_asset_to_peg:
                if this_price and this_price > 0:
                    RpcCacheStorage.set_cache_with_ttl(
                        asset_source,
                        "ASSET_TO_PEG_PRICE_FUTURE",
                        this_price,
                        ttl=NETWORK_BLOCK_TIME,
                    )
            else:
                if this_price and this_price > 0:
                    RpcCacheStorage.set_cache_with_ttl(
                        asset_source,
                        "PEG_TO_BASE_PRICE_FUTURE",
                        this_price,
                        ttl=NETWORK_BLOCK_TIME,
                    )

    elif asset_source_type in (
        AssetSourceType.EACAggregatorProxy,
        AssetSourceType.PriceCapAdapterStable,
        AssetSourceType.PendlePriceCapAdapter,
        AssetSourceType.CLrETHSynchronicityPriceAdapter,
        AssetSourceType.CLrETHSynchronicityPriceAdapterPegToBase,
        AssetSourceType.sDAIMainnetPriceCapAdapter,
        AssetSourceType.PriceCapAdapter,
        AssetSourceType.WstETHPriceCapAdapter,
        AssetSourceType.WeETHPriceCapAdapter,
        AssetSourceType.SUSDePriceCapAdapter,
        AssetSourceType.RsETHPriceCapAdapter,
        AssetSourceType.RETHPriceCapAdapter,
        AssetSourceType.OsETHPriceCapAdapter,
        AssetSourceType.CbETHPriceCapAdapter,
        AssetSourceType.GhoOracle,
        AssetSourceType.EthXPriceCapAdapter,
        AssetSourceType.EBTCPriceCapAdapter,
        AssetSourceType.EUSDePriceCapAdapter,
        AssetSourceType.WstETHSynchronicityPriceAdapter,
        AssetSourceType.sDAISynchronicityPriceAdapter,
        AssetSourceType.EURPriceCapAdapterStable,
    ):
        if event:
            price = int(event.args.answer)
        elif transaction:
            price = int(transaction["median_price"])

    else:
        send_unsupported_asset_source_notification(
            asset_source, f"Unsupported in Numerator {asset_source_type}"
        )
        raise ValueError(
            f"Unknown asset source type: {asset_source_type} - {asset_source}"
        )

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        get_blockNumber(event, transaction),
        price,
    ]
