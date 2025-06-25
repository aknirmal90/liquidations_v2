from django.core.cache import cache

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource


class CLSynchronicityPriceAdapterPegToBaseAssetSource(BaseEthereumAssetSource):
    @property
    def asset_to_peg_source(self):
        asset_to_peg = self.asset_to_peg_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=asset_to_peg)

    @property
    def peg_to_base_source(self):
        peg_to_base = self.peg_to_base_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=peg_to_base)

    @property
    def asset_to_peg_source_address(self):
        return self._get_cached_property("ASSET_TO_PEG")

    @property
    def peg_to_base_source_address(self):
        return self._get_cached_property("PEG_TO_BASE")

    @property
    def events(self):
        return self.asset_to_peg_source.events + self.peg_to_base_source.events

    @property
    def method_ids(self):
        return self.asset_to_peg_source.method_ids + self.peg_to_base_source.method_ids

    def get_underlying_sources_to_monitor(self):
        return (
            self.asset_to_peg_source.get_underlying_sources_to_monitor()
            + self.peg_to_base_source.get_underlying_sources_to_monitor()
        )

    def get_event_price(self, event: dict, is_synthetic: bool = False) -> int:
        asset_to_peg_price = self.get_event_price_from_asset_to_peg(event)
        peg_to_base_price = self.get_event_price_from_peg_to_base(event)
        price = int(
            (asset_to_peg_price * peg_to_base_price * 10**self.DECIMALS)
            / self.DENOMINATOR
        )
        return price

    def get_event_price_from_asset_to_peg(self, event: dict) -> int:
        if (
            event.address.lower()
            in self.asset_to_peg_source.get_underlying_sources_to_monitor()
        ):
            cache_key = self.local_cache_key("asset_to_peg_price")
            cache.set(cache_key, event.args.answer)
            return event.args.answer
        elif (
            event.address.lower()
            in self.peg_to_base_source.get_underlying_sources_to_monitor()
        ):
            cache_key = self.local_cache_key("asset_to_peg_price")
            asset_to_peg_price = cache.get(cache_key)
            if asset_to_peg_price is None:
                asset_to_peg_price = event.args.answer
                cache.set(cache_key, asset_to_peg_price)
            return asset_to_peg_price

    def get_event_price_from_peg_to_base(self, event: dict) -> int:
        if (
            event.address.lower()
            in self.peg_to_base_source.get_underlying_sources_to_monitor()
        ):
            cache_key = self.local_cache_key("peg_to_base_price")
            cache.set(cache_key, event.args.answer)
            return event.args.answer
        elif (
            event.address.lower()
            in self.asset_to_peg_source.get_underlying_sources_to_monitor()
        ):
            cache_key = self.local_cache_key("peg_to_base_price")
            underlying_price = cache.get(cache_key)
            if underlying_price is None:
                underlying_price = event.args.answer
                cache.set(cache_key, underlying_price)
            return underlying_price

    @property
    def DECIMALS(self):
        return self._get_cached_property("DECIMALS")

    @property
    def DENOMINATOR(self):
        return self._get_cached_property("DENOMINATOR")
