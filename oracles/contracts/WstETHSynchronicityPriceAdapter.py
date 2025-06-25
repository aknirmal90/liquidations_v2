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

    def get_event_price(self, event: dict) -> int:
        underlying_price = self.underlying_asset_source.get_event_price(event)
        ratio = self.get_ratio(use_parameter=True)
        return int((underlying_price * ratio) / (10**self.RATIO_DECIMALS))

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
