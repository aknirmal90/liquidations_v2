from django.core.cache import cache

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource, RatioProviderMixin


class sDAISynchronicityPriceAdapterAssetSource(BaseEthereumAssetSource, RatioProviderMixin):
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

    def get_event_price(self, event: dict, is_synthetic: bool = False) -> int:
        underlying_price = self.underlying_asset_source.get_event_price(event, is_synthetic)
        if not is_synthetic:
            cache.set(self.local_cache_key("underlying_price"), underlying_price)
        ratio = self.get_ratio()
        return int((underlying_price * ratio) / (10**self.RATIO_DECIMALS))

    @property
    def RATIO_PROVIDER_METHOD(self):
        return "chi"

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "RATE_PROVIDER"
