from typing import Dict, List, Optional

from oracles.contracts.base import BaseEthereumAssetSource
from utils.encoding import decode_any


class AggregatorAssetSource(BaseEthereumAssetSource):
    def get_numerator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the numerator for price calculation.
        For events: uses event.args.answer
        For transactions: uses transaction["price"]
        For current price: uses latest price from RPC
        """
        if event:
            return event.args.answer
        elif transaction:
            return transaction["price"]
        else:
            raise ValueError("No event or transaction provided")

    def get_underlying_sources_to_monitor(self) -> List[str]:
        return [decode_any(self.asset_source)]
