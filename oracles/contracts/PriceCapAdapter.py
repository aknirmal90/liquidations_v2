from datetime import datetime
from typing import Dict, Optional

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource, RatioProviderMixin
from utils.rpc import get_evm_block_timestamps


class PriceCapAdapterAssetSource(BaseEthereumAssetSource, RatioProviderMixin):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("BASE_TO_USD_AGGREGATOR")

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
        return 10**self.RATIO_DECIMALS * 1000

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Uses the ratio from the ratio provider, capped by max ratio.
        """
        block_number = None
        if event:
            block_number = getattr(event, "blockNumber", None)

        current_ratio = self.get_ratio(block_number=block_number)
        max_ratio = self.get_max_cap(event, transaction)

        if current_ratio > max_ratio:
            current_ratio = max_ratio

        return current_ratio

    @property
    def MAX_RATIO_GROWTH_PER_SECOND(self):
        return self._get_cached_property("getMaxRatioGrowthPerSecond")

    @property
    def SNAPSHOT_TIMESTAMP(self):
        return self._get_cached_property("getSnapshotTimestamp")

    @property
    def SNAPSHOT_RATIO(self):
        return self._get_cached_property("getSnapshotRatio")

    def get_max_cap(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        if event:
            block_number = event.blockNumber
            block_timestamp = get_evm_block_timestamps([block_number])[block_number]
        elif transaction:
            block_timestamp = int(datetime.now().timestamp())
        else:
            raise ValueError("No event or transaction provided")

        return self.SNAPSHOT_RATIO + self.MAX_RATIO_GROWTH_PER_SECOND * (
            block_timestamp - self.SNAPSHOT_TIMESTAMP
        )
