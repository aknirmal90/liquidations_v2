from oracles.contracts.base import RatioProviderMixin
from oracles.contracts.CLSynchronicityPriceAdapterPegToBase import (
    CLSynchronicityPriceAdapterPegToBaseAssetSource,
)


class CLwstETHSynchronicityPriceAdapterAssetSource(
    CLSynchronicityPriceAdapterPegToBaseAssetSource, RatioProviderMixin
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

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "STETH"

    def get_event_price(self, event):
        price = super().get_event_price(event)
        return int(
            (price * self.get_ratio(use_parameter=True)) / (10**self.RATIO_DECIMALS)
        )
