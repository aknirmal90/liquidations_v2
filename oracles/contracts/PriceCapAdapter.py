from django.core.cache import cache
from web3 import Web3

from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource
from utils.rpc import get_evm_block_timestamps, rpc_adapter


class PriceCapAdapterAssetSource(BaseEthereumAssetSource):
    def get_base_abi(self):
        abi = [
            {
                "inputs": [],
                "name": self.RATIO_PROVIDER_METHOD,
                "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function",
            }
        ]
        return abi

    @property
    def RATIO_PROVIDER_ABI(self):
        return self.get_base_abi()

    @property
    def RATIO_PROVIDER_METHOD(self):
        raise NotImplementedError("RATIO_PROVIDER_METHOD is not implemented")

    @property
    def RATIO_PROVIDER_ADDRESS_NAME(self):
        return "RATIO_PROVIDER"

    @property
    def underlying_asset_source(self):
        underlying = self.underlying_asset_source_address
        return AggregatorProxyAssetSource(asset=self.asset, asset_source=underlying)

    @property
    def underlying_asset_source_address(self):
        return self._get_cached_property("BASE_TO_USD_AGGREGATOR")

    @property
    def events(self):
        return self.underlying_asset_source.events

    @property
    def method_ids(self):
        return self.underlying_asset_source.method_ids

    def get_underlying_sources_to_monitor(self):
        return self.underlying_asset_source.get_underlying_sources_to_monitor()

    def get_event_price(self, event: dict) -> int:
        max_ratio = self.get_max_ratio(event)
        current_ratio = self.get_ratio()

        if current_ratio > max_ratio:
            current_ratio = max_ratio

        base_price = self.underlying_asset_source.get_event_price(event)
        return int((base_price * current_ratio) / 10**self.RATIO_DECIMALS)

    def get_ratio(self):
        cache_key = self.local_cache_key("ratio")
        ratio = cache.get(cache_key)
        if ratio is None:
            contract = rpc_adapter.client.eth.contract(
                address=Web3.to_checksum_address(self.RATIO_PROVIDER),
                abi=self.RATIO_PROVIDER_ABI,
            )
            func = getattr(contract.functions, self.RATIO_PROVIDER_METHOD)
            ratio = func().call()
            cache.set(cache_key, ratio)
        return ratio

    @property
    def MAX_RATIO_GROWTH_PER_SECOND(self):
        return self._get_cached_property("getMaxRatioGrowthPerSecond")

    @property
    def MAX_RATIO_GROWTH_PER_YEAR(self):
        return self._get_cached_property("getMaxYearlyGrowthRatePercent")

    @property
    def RATIO_DECIMALS(self):
        return self._get_cached_property("RATIO_DECIMALS")

    @property
    def RATIO_PROVIDER(self):
        return self._get_cached_property(self.RATIO_PROVIDER_ADDRESS_NAME)

    @property
    def SNAPSHOT_TIMESTAMP(self):
        return self._get_cached_property("getSnapshotTimestamp")

    @property
    def SNAPSHOT_RATIO(self):
        return self._get_cached_property("getSnapshotRatio")

    def get_max_ratio(self, event: dict) -> int:
        block_number = event.blockNumber
        block_timestamp = get_evm_block_timestamps([block_number])[block_number]
        return self.SNAPSHOT_RATIO + self.MAX_RATIO_GROWTH_PER_SECOND * (
            block_timestamp - self.SNAPSHOT_TIMESTAMP
        )
