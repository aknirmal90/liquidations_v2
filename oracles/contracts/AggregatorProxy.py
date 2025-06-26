from typing import Dict, Optional

from oracles.contracts.Aggregator import AggregatorAssetSource
from oracles.contracts.base import BaseEthereumAssetSource


class AggregatorProxyAssetSource(BaseEthereumAssetSource):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("aggregator")

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
        Delegates to the underlying asset source.
        """
        return self.underlying_asset_source.get_numerator(event, transaction)
