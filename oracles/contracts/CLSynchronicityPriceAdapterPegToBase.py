from typing import Dict, Optional

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
        return self._get_cached_property("ASSET_TO_PEG_ADDRESS", "ASSET_TO_PEG")

    @property
    def peg_to_base_source_address(self):
        return self._get_cached_property("PEG_TO_BASE_ADDRESS", "PEG_TO_BASE")

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

    def get_numerator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the asset to peg price.
        """
        cache_key = self.local_cache_key("asset_to_peg_price")
        if event:
            if (
                event.address.lower()
                in self.asset_to_peg_source.get_underlying_sources_to_monitor()
            ):
                cache.set(cache_key, event.args.answer)
                return event.args.answer
            else:
                asset_to_peg_price = cache.get(cache_key)
                if asset_to_peg_price is None:
                    asset_to_peg_price = 0
                return asset_to_peg_price
        else:
            return cache.get(cache_key)

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the peg to base price.
        """
        cache_key = self.local_cache_key("peg_to_base_price")
        if event:
            if (
                event.address.lower()
                in self.peg_to_base_source.get_underlying_sources_to_monitor()
            ):
                cache.set(cache_key, event.args.answer)
                return event.args.answer
            else:
                peg_to_base_price = cache.get(cache_key)
                if peg_to_base_price is None:
                    peg_to_base_price = 0
                return peg_to_base_price
        else:
            return cache.get(cache_key)

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Uses the multiplier from the contract.
        """
        return 10**self.DECIMALS

    @property
    def DECIMALS(self):
        return self._get_cached_property("DECIMALS")

    @property
    def DENOMINATOR(self):
        return self._get_cached_property("DENOMINATOR")
