from django.core.cache import cache
from web3 import Web3

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource
from utils.rpc import rpc_adapter


class WstETHSynchronicityPriceAdapterAssetSource(BaseEthereumAssetSource):
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
        # This implementation necessarily requires the asset cap event to sync first to
        # get historially correct prices.
        underlying_price = self.underlying_asset_source.get_event_price(event)
        ratio = self.get_ratio()
        return int((underlying_price * ratio) / (10**self.RATIO_DECIMALS))

    @property
    def RATIO_DECIMALS(self):
        return self._get_cached_property("RATIO_DECIMALS")

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "STETH"

    @property
    def RATIO_PROVIDER(self):
        return self._get_cached_property(self.RATIO_PROVIDER_ADDRESS_NAME)

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

    @property
    def RATIO_PROVIDER_METHOD(self):
        return "getPooledEthByShares"

    def get_ratio(self):
        cache_key = self.local_cache_key("ratio")
        ratio = cache.get(cache_key)
        if ratio is None:
            contract = rpc_adapter.client.eth.contract(
                address=Web3.to_checksum_address(self.RATIO_PROVIDER),
                abi=self.RATIO_PROVIDER_ABI,
            )
            func = getattr(contract.functions, self.RATIO_PROVIDER_METHOD)
            ratio = func(10**self.RATIO_DECIMALS).call()
            cache.set(cache_key, ratio)
        return ratio
