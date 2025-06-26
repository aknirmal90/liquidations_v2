from typing import Dict, Optional

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource, RatioProviderMixin


class WstETHSynchronicityPriceAdapterAssetSource(
    BaseEthereumAssetSource, RatioProviderMixin
):
    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("ETH_TO_BASE")

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
        Uses the ratio from the ratio provider.
        """
        block_number = None
        if event:
            block_number = getattr(event, "blockNumber", None)
        return self.get_ratio(use_parameter=True, block_number=block_number)

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
        return "getPooledEthByShares"

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "STETH"

    @property
    def RATIO_PROVIDER_ABI(self):
        abi = [
            {
                "constant": True,
                "inputs": [{"name": "_sharesAmount", "type": "uint256"}],
                "name": "getPooledEthByShares",
                "outputs": [{"name": "", "type": "uint256"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function",
            }
        ]
        return abi
