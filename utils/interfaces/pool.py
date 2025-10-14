from typing import Dict, List

from decouple import config

from utils.interfaces.base import BaseContractInterface


class PoolInterface(BaseContractInterface):
    def __init__(self):
        pool_address = config("POOL_V3_POOL")
        super().__init__(pool_address)
        # Keep backward compatibility alias
        self.pool_address = self.contract_address

    def get_user_emode(self, users: List[str]) -> Dict[str, int]:
        """
        Issues a batch of getUserEMode(address) calls and decodes the results.
        Returns a dict mapping user address to eMode category ID.

        Args:
            users: List of user addresses to query

        Returns:
            Dictionary mapping user address to eMode category ID (uint256)
        """
        call_targets = [
            {
                "method_signature": "getUserEMode(address)",
                "param_types": ["address"],
                "params": [user],
            }
            for user in users
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            return self.decode_eth_call_result(hex_result, "getUserEMode")

        results_by_user = {}
        if isinstance(raw_results, list):
            for user, rpc_result in zip(users, raw_results):
                result_value = rpc_result.get("result", "")
                results_by_user[user] = decode_result(result_value)
            return results_by_user
        return raw_results
