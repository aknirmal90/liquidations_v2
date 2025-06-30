from typing import Optional

from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class OsETHPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
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

    def get_ratio(
        self, use_parameter=False, parameter=None, block_number: Optional[int] = None
    ):
        return super().get_ratio(use_parameter=True, block_number=block_number)
