from typing import Dict, Optional

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

    def get_numerator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the numerator for price calculation.
        Uses the underlying asset source's price as the base.
        """
        return self.underlying_asset_source.get_numerator(event, transaction)

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Applies the discount based on time to maturity.
        """
        current_discount = self.get_current_discount(event, transaction)
        return self.PERCENTAGE_FACTOR - current_discount

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the denominator for price calculation.
        Uses the percentage factor.
        """
        return self.PERCENTAGE_FACTOR * self.SECONDS_PER_YEAR

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

    def get_current_discount(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        cache_key = self.local_cache_key("CURRENT_DISCOUNT")
        if event:
            block_number = event.blockNumber
            timestamp = get_evm_block_timestamps([block_number])[block_number]
            time_to_maturity = max(self.MATURITY - timestamp, 0)
            current_discount = int(self.DISCOUNT_RATE_PER_YEAR * time_to_maturity)
            cache.set(cache_key, current_discount)
            return current_discount
        elif transaction:
            return cache.get(cache_key)
        else:
            raise ValueError("No event or transaction provided")
