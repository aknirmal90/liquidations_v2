from oracles.contracts.utils import RpcCacheStorage
from utils.clickhouse.client import clickhouse_client


class PriceOracleInterface:
    def __init__(self, asset: str, asset_source: str):
        self.asset = asset
        self.asset_source = asset_source
        self.name, self.abi = RpcCacheStorage.get_contract_info(asset_source)

    @property
    def latest_price_from_rpc(self) -> float:
        return RpcCacheStorage.call_function(
            self.asset_source, "latestAnswer", abi=self.abi
        )

    @property
    def historical_price_from_event(self) -> float:
        result = clickhouse_client.execute_query(
            f"SELECT historical_price FROM aave_ethereum.LatestPriceEvent WHERE asset = '{self.asset}' AND asset_source = '{self.asset_source}' ORDER BY blockTimestamp DESC LIMIT 1"
        )
        if result.result_rows:
            return result.result_rows[0][0]
        else:
            return None

    @property
    def historical_price_from_transaction(self) -> float:
        result = clickhouse_client.execute_query(
            f"SELECT historical_price FROM aave_ethereum.LatestPriceTransaction WHERE asset = '{self.asset}' AND asset_source = '{self.asset_source}' ORDER BY blockTimestamp DESC LIMIT 1"
        )
        if result.result_rows:
            return result.result_rows[0][0]
        else:
            return None

    @property
    def predicted_price_from_transaction(self) -> float:
        result = clickhouse_client.execute_query(
            f"SELECT predicted_price FROM aave_ethereum.LatestPriceTransaction WHERE asset = '{self.asset}' AND asset_source = '{self.asset_source}' ORDER BY blockTimestamp DESC LIMIT 1"
        )
        if result.result_rows:
            return result.result_rows[0][0]
        else:
            return None
