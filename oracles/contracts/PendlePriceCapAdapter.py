from django.core.cache import cache

from oracles.contracts.base import BaseEthereumAssetSource
from oracles.contracts.PriceCapAdapterStable import PriceCapAdapterStableAssetSource
from utils.rpc import get_evm_block_timestamps


class PendlePriceCapAdapterAssetSource(BaseEthereumAssetSource):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return PriceCapAdapterStableAssetSource(
            asset=self.asset, asset_source=underlying
        )

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

    def get_event_price(self, event: dict, is_synthetic: bool = False) -> int:
        underlying_price = self.underlying_asset_source.get_event_price(event, is_synthetic)
        if not is_synthetic:
            cache.set(self.local_cache_key("underlying_price"), underlying_price)
        current_discount = self.getCurrentDiscount(event)
        price = (
            underlying_price
            * (self.PERCENTAGE_FACTOR - current_discount)
            / self.PERCENTAGE_FACTOR
        )
        return int(price)

    @property
    def SECONDS_PER_YEAR(self):
        return self._get_cached_property("SECONDS_PER_YEAR")

    @property
    def MATURITY(self):
        return self._get_cached_property("MATURITY")

    @property
    def DISCOUNT_RATE_PER_YEAR(self):
        return self._get_cached_property("discountRatePerYear")

    @property
    def PERCENTAGE_FACTOR(self):
        return self._get_cached_property("PERCENTAGE_FACTOR")

    def getCurrentDiscount(self, event):
        block_number = event.blockNumber
        timestamp = get_evm_block_timestamps([block_number])[block_number]
        time_to_maturity = max(self.MATURITY - timestamp, 0)
        return int(
            self.DISCOUNT_RATE_PER_YEAR * time_to_maturity / self.SECONDS_PER_YEAR
        )
