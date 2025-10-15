from typing import List

from decouple import config

from utils.interfaces.base import BaseContractInterface


class AaveToken(BaseContractInterface):
    def __init__(self, contract_address: str):
        self.contract_address = contract_address
        self.rpc_url = config("NETWORK_RPC")
        self.abi = [
            {
                "inputs": [
                    {"internalType": "address", "name": "user", "type": "address"}
                ],
                "name": "scaledBalanceOf",
                "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function",
            },
        ]

    def get_scaled_balance(self, users: List[str]) -> dict:
        """
        Returns a dict mapping user address to scaledBalanceOf for each provided user.

        Args:
            users: List of user addresses.

        Returns:
            Dictionary {user_address: scaled_balance}
        """
        call_targets = [
            {
                "method_signature": "scaledBalanceOf(address)",
                "param_types": ["address"],
                "params": [user],
            }
            for user in users
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            if not hex_result or hex_result == "0x":
                return 0
            return int(hex_result, 16)

        results_by_user = {}
        if isinstance(raw_results, list):
            for user, rpc_result in zip(users, raw_results):
                result_value = rpc_result.get("result", "")
                results_by_user[user] = decode_result(result_value)
            return results_by_user
        return raw_results
