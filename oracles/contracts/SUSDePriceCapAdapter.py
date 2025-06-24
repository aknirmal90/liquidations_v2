from django.core.cache import cache
from web3 import Web3

from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource
from oracles.contracts.PriceCapAdapterStable import PriceCapAdapterStableAssetSource
from utils.rpc import rpc_adapter


class SUSDePriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
    @property
    def RATIO_PROVIDER_ABI(self):
        abi = [
            {
                "inputs": [
                    {"internalType": "uint256", "name": "shares", "type": "uint256"}
                ],
                "name": "convertToAssets",
                "outputs": [
                    {"internalType": "uint256", "name": "assets", "type": "uint256"}
                ],
                "stateMutability": "view",
                "type": "function",
            }
        ]
        return abi

    @property
    def RATIO_PROVIDER_METHOD(self):
        return "convertToAssets"

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
            cache.set(cache_key, ratio, 60)
        return ratio

    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return PriceCapAdapterStableAssetSource(
            asset=self.asset, asset_source=underlying
        )
