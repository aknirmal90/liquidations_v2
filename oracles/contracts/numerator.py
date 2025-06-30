from oracles.contracts.utils import AssetSourceType, RpcCacheStorage, get_timestamp
from utils.constants import NETWORK_BLOCK_TIME
from utils.encoding import decode_any


def get_numerator(asset: str, asset_source: str, event=None, transaction=None) -> int:
    asset_source = decode_any(asset_source)
    asset_source_type, abi = RpcCacheStorage.get_contract_info(asset_source)

    if asset_source_type in (
        AssetSourceType.CLSynchronicityPriceAdapterPegToBase,
        AssetSourceType.CLwstETHSynchronicityPriceAdapter,
    ):
        DECIMALS = RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "DECIMALS"
        )
        if event:
            price = int(event.args.answer)
            address = decode_any(event.address)
        elif transaction:
            price = int(transaction["median_price"])
            address = decode_any(transaction["oracle_address"])

        is_asset_to_peg = address == RpcCacheStorage.get_cached_asset_source_function(
            asset_source, "ASSET_TO_PEG"
        )
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

        if other_price_past is None:
            other_price_past = 0

        if event:
            price = (price * other_price_past) * (10**DECIMALS)
            if is_asset_to_peg:
                RpcCacheStorage.set_cache(asset_source, "ASSET_TO_PEG_PRICE", price)
            else:
                RpcCacheStorage.set_cache(asset_source, "PEG_TO_BASE_PRICE", price)
        elif transaction:
            if other_price_future:
                price = (price * other_price_future) * (10**DECIMALS)
            else:
                price = (price * other_price_past) * (10**DECIMALS)

            if is_asset_to_peg:
                RpcCacheStorage.set_cache_with_ttl(
                    asset_source,
                    "ASSET_TO_PEG_PRICE_FUTURE",
                    price,
                    ttl=NETWORK_BLOCK_TIME,
                )
            else:
                RpcCacheStorage.set_cache_with_ttl(
                    asset_source,
                    "PEG_TO_BASE_PRICE_FUTURE",
                    price,
                    ttl=NETWORK_BLOCK_TIME,
                )

    else:
        if event:
            price = int(event.args.answer)
        elif transaction:
            price = int(transaction["median_price"])

    return [
        asset,
        asset_source,
        asset_source_type,
        get_timestamp(event, transaction),
        price,
    ]
