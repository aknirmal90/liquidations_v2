from oracles.contracts.base import BaseEthereumAssetSource


class GhoOracleAssetSource(BaseEthereumAssetSource):
    def get_event_price(self, event: dict) -> int:
        return 10**8

    @property
    def events(self):
        return []

    @property
    def method_ids(self):
        return []

    def get_underlying_sources_to_monitor(self):
        return [self.asset_source]
