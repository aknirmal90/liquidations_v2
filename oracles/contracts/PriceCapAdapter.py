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

    def get_event_price(self, event: dict) -> int:
        max_ratio = self.get_max_ratio(event)
        current_ratio = self.get_ratio()

        if current_ratio > max_ratio:
            current_ratio = max_ratio

        base_price = self.underlying_asset_source.get_event_price(event)
        return int((base_price * current_ratio) / 10**self.RATIO_DECIMALS)

    @property
    def MAX_RATIO_GROWTH_PER_SECOND(self):
        return self._get_cached_property("getMaxRatioGrowthPerSecond")

    @property
    def SNAPSHOT_TIMESTAMP(self):
        return self._get_cached_property("getSnapshotTimestamp")

    @property
    def SNAPSHOT_RATIO(self):
        return self._get_cached_property("getSnapshotRatio")

    def get_max_ratio(self, event: dict) -> int:
        block_number = event.blockNumber
        block_timestamp = get_evm_block_timestamps([block_number])[block_number]
        return self.SNAPSHOT_RATIO + self.MAX_RATIO_GROWTH_PER_SECOND * (
            block_timestamp - self.SNAPSHOT_TIMESTAMP
        )
