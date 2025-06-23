from django.core.cache import cache

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource


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
        return self.underlying_asset_source.events + ["PriceCapUpdated"]

    @property
    def method_ids(self):
        return self.underlying_asset_source.method_ids

    def get_underlying_sources_to_monitor(self):
        return self.underlying_asset_source.get_underlying_sources_to_monitor() + [
            self.asset_source
        ]

    def get_event_price(self, event: dict) -> int:
        # This implementation necessarily requires the asset cap event to sync first to
        # get historially correct prices.
        underlying_price = self.get_event_price_from_underlying(event)
        asset_cap = self.get_event_price_from_asset_cap(event)

        if underlying_price > asset_cap:
            return asset_cap
        return underlying_price

    def get_event_price_from_underlying(self, event: dict) -> int:
        if event.event == "NewTransmission":
            # NewTransmission is the underlying asset source price
            cache_key = self.local_cache_key("underlying_price")
            cache.set(cache_key, event.args.answer)
            return event.args.answer
        elif event.event == "PriceCapUpdated":
            # When `underlying_price` is not set, initialize it to most recent max cap
            # This is factually incorrect, but it's the best we can do for now
            cache_key = self.local_cache_key("underlying_price")
            max_cap = cache.get(cache_key)
            if max_cap is None:
                max_cap = event.args.priceCap
                cache.set(cache_key, max_cap)
            return max_cap

    def get_event_price_from_asset_cap(self, event: dict) -> int:
        if event.event == "PriceCapUpdated":
            # PriceCapUpdated is the asset cap
            cache_key = self.local_cache_key("asset_cap")
            cache.set(cache_key, event.args.priceCap)
            return event.args.priceCap
        elif event.event == "NewTransmission":
            # When `asset_cap` is not set, initialize it to the most recent underlying asset source price
            # This is factually incorrect, but it's the best we can do for now
            cache_key = self.local_cache_key("asset_cap")
            underlying_price = cache.get(cache_key)
            if underlying_price is None:
                underlying_price = event.args.answer
                cache.set(cache_key, underlying_price)
            return underlying_price
