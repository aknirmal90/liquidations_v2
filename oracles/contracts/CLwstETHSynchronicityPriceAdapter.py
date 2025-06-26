from typing import Dict, Optional

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

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Uses the base price from parent class and applies additional ratio.
        """
        # Get the base multiplier from parent class
        base_multiplier = super().get_multiplier(event, transaction)
        # Apply additional ratio
        block_number = None
        if event:
            block_number = getattr(event, "blockNumber", None)
        return base_multiplier * self.get_ratio(
            use_parameter=True, block_number=block_number
        )

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the denominator for price calculation.
        Uses the base denominator from parent class and applies ratio decimals.
        """
        # Get the base denominator from parent class
        base_denominator = super().get_denominator(event, transaction)
        # Apply ratio decimals
        return base_denominator * 10**self.RATIO_DECIMALS
