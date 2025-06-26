from typing import Dict, Optional

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource, RatioProviderMixin


class sDAISynchronicityPriceAdapterAssetSource(
    BaseEthereumAssetSource, RatioProviderMixin
):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("DAI_TO_USD")

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

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the denominator for price calculation.
        Uses the ratio decimals.
        """
        return 10**self.RATIO_DECIMALS

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Uses the ratio from the ratio provider.
        """
        block_number = None
        if event:
            block_number = getattr(event, "blockNumber", None)
        return self.get_ratio(block_number=block_number)

    @property
    def RATIO_PROVIDER_METHOD(self):
        return "chi"

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "RATE_PROVIDER"
