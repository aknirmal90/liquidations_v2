from typing import Dict, List, Optional

from oracles.contracts.base import BaseEthereumAssetSource
from utils.encoding import decode_any


class AggregatorAssetSource(BaseEthereumAssetSource):
    @property
    def events(self) -> List[str]:
        # NewTransmission (
        # index_topic_1
        # uint32 aggregatorRoundId,
        # int192 answer,
        # address transmitter,
        # uint32 observationsTimestamp,
        # int192[] observations,
        # bytes observers,
        # int192 juelsPerFeeCoin,
        # bytes32 configDigest,
        # uint40 epochAndRound
        # )
        return [
            "NewTransmission",
        ]

    @property
    def method_ids(self) -> List[str]:
        # forward(address to, bytes data)
        return [
            "0x6fadcf72",
        ]

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

    def get_denominator(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the denominator for price calculation.
        Default implementation returns 1 (no division).
        """
        return 1

    def get_multiplier(
        self, event: Optional[Dict] = None, transaction: Optional[Dict] = None
    ) -> int:
        """
        Get the multiplier for price calculation.
        Default implementation returns 1 (no multiplication).
        """
        return 1

    def get_underlying_sources_to_monitor(self) -> List[str]:
        return [decode_any(self.asset_source)]
