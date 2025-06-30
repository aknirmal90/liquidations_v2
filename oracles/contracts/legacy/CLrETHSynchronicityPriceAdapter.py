from typing import Dict, Optional

from django.core.cache import cache

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource, RatioProviderMixin


class CLrETHSynchronicityPriceAdapterAssetSource(
    BaseEthereumAssetSource, RatioProviderMixin
):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("ETH_TO_USD")

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
        Uses the ratio from the ratio provider.
        """
        block_number = None
        cache_key = self.local_cache_key("RATIO")
        if event:
            block_number = getattr(event, "blockNumber", None)
            ratio = self.get_ratio(block_number=block_number)
            cache.set(cache_key, ratio)
            return ratio
        else:
            return cache.get(cache_key)

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the denominator for price calculation.
        Uses the ratio decimals.
        """
        return 10**self.RATIO_DECIMALS

    @property
    def RATIO_PROVIDER_METHOD(self):
        return "getExchangeRate"

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "RETH"
