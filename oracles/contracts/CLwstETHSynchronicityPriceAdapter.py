from django.core.cache import cache
from web3 import Web3

from oracles.contracts.CLSynchronicityPriceAdapterPegToBase import (
    CLSynchronicityPriceAdapterPegToBaseAssetSource,
)
from utils.rpc import rpc_adapter


class CLwstETHSynchronicityPriceAdapterAssetSource(
    CLSynchronicityPriceAdapterPegToBaseAssetSource
):
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

    @property
    def RATIO_DECIMALS(self):
        return self._get_cached_property("RATIO_DECIMALS")

    @property
    def RATIO_PROVIDER(self):
        return self._get_cached_property(self.RATIO_PROVIDER_ADDRESS_NAME)

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "STETH"

    def get_event_price(self, event):
        price = super().get_event_price(event)
        return int((price * self.get_ratio()) / (10**self.RATIO_DECIMALS))
