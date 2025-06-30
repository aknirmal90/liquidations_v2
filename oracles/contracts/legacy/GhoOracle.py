from typing import Dict, Optional

from oracles.contracts.base import BaseEthereumAssetSource


class GhoOracleAssetSource(BaseEthereumAssetSource):
    def get_numerator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the numerator for price calculation.
        GHO has a fixed price of 1 USD (10^8 in 8 decimals).
        """
        return 10**8

    @property
    def events(self):
        return []

    @property
    def method_ids(self):
        return []

    def get_underlying_sources_to_monitor(self):
        return [self.asset_source]
