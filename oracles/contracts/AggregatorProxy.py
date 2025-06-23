from django.core.cache import cache

from oracles.contracts.Aggregator import AggregatorAssetSource
from oracles.contracts.base import BaseEthereumAssetSource


class AggregatorProxyAssetSource(BaseEthereumAssetSource):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        cache_key = self.local_cache_key("underlying_asset_source")
        underlying_asset_source = cache.get(cache_key)
        if underlying_asset_source is None:
            underlying_asset_source = self.call_function("aggregator")
            cache.set(cache_key, underlying_asset_source)
        return underlying_asset_source

    @property
    def events(self):
        return self.underlying_asset_source.events

    @property
    def method_ids(self):
        return self.underlying_asset_source.method_ids

    def get_underlying_sources_to_monitor(self):
        return self.underlying_asset_source.get_underlying_sources_to_monitor()

    def get_event_price(self, event: dict) -> int:
        return self.underlying_asset_source.get_event_price(event)
