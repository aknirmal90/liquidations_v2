from typing import Optional

from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class WstETHPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
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

    def get_ratio(
        self, use_parameter=False, parameter=None, block_number: Optional[int] = None
    ):
        return super().get_ratio(use_parameter=True, block_number=block_number)
