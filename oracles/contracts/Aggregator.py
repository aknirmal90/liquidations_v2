from typing import List


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

    def get_event_price(self, event: dict) -> int:
        return event.args.answer

    def get_transaction_price(self, transaction: dict) -> int:
        return transaction["price"]

    def get_underlying_sources_to_monitor(self) -> List[str]:
        return [decode_any(self.asset_source)]
