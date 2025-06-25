from django.core.cache import cache
from web3 import Web3

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource
from utils.encoding import decode_any
from utils.rpc import rpc_adapter


class PriceCapAdapterStableAssetSource(BaseEthereumAssetSource):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("ASSET_TO_USD_AGGREGATOR")

    @property
    def events(self):
        return self.underlying_asset_source.events

    @property
    def method_ids(self):
        return self.underlying_asset_source.method_ids

    def get_underlying_sources_to_monitor(self):
        return self.underlying_asset_source.get_underlying_sources_to_monitor()

    def get_event_price(self, event: dict) -> int:
        asset_cap = self.MAX_CAP
        underlying_price = event.args.answer

        if underlying_price > asset_cap:
            return asset_cap
        return underlying_price

    @property
    def MAX_CAP(self):
        MAX_CAP = cache.get(self.local_cache_key("MAX_CAP"))
        if MAX_CAP is not None:
            return MAX_CAP

        events = rpc_adapter.extract_raw_event_data(
            topics=[
                "0xa89f50d1caf6c404765ce94b422be388ce69c8ed68921620fa6a83c810000615"
            ],
            contract_addresses=[Web3.to_checksum_address(self.asset_source)],
            start_block=0,
            end_block=rpc_adapter.block_height,
        )
        latest_event = events[-1]
        cache_key = self.local_cache_key("MAX_CAP")
        data = decode_any(latest_event.data)
        cache.set(cache_key, int(data, 16))
        return int(data, 16)
