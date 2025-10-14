from typing import Dict, List, Tuple

from decouple import config
from eth_abi import decode

from utils.interfaces.base import BaseContractInterface


class DataProviderInterface(BaseContractInterface):
    def __init__(self):
        data_provider_address = config("POOL_V3_DATAPROVIDER")
        super().__init__(data_provider_address)
        # Keep backward compatibility alias
        self.data_provider_address = self.contract_address

    def get_reserves_configuration(self, assets: List[str]) -> dict:
        """
        Issues a batch of getReserveConfigurationData(address) calls and decodes the results.
        Returns a dict mapping asset address to result.
        """
        call_targets = [
            {
                "method_signature": "getReserveConfigurationData(address)",
                "param_types": ["address"],
                "params": [asset],
            }
            for asset in assets
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            return self.decode_eth_call_result(
                hex_result, "getReserveConfigurationData"
            )

        results_by_asset = {}
        if isinstance(raw_results, list):
            for asset, rpc_result in zip(assets, raw_results):
                result_value = rpc_result.get("result", "")
                results_by_asset[asset] = decode_result(result_value)
            return results_by_asset
        return raw_results

    def get_all_atokens(self) -> List[tuple]:
        """
        Returns a list of all aToken tuples (symbol, tokenAddress) by calling getAllATokens().

        The getAllATokens() method returns an array of TokenData structs:
        struct TokenData {
            string symbol;
            address tokenAddress;
        }

        Returns:
            List of tuples containing (symbol, tokenAddress)
        """
        call_targets = [
            {
                "method_signature": "getAllATokens()",
                "param_types": [],
                "params": [],
            }
        ]
        raw_results = self.batch_eth_call(call_targets)
        result = raw_results[0]["result"]

        try:
            # Try to use the ABI-based decoding first
            return self.decode_eth_call_result(result, "getAllATokens")
        except Exception:
            # If ABI decoding fails, manually decode the struct array
            # getAllATokens returns (string,address)[] which is a dynamic array of tuples
            data = bytes.fromhex(result[2:] if result.startswith("0x") else result)

            # Manually decode: the output is an array of (string, address) tuples
            # The proper type string for eth_abi is "(string,address)[]"
            decoded = decode(["(string,address)[]"], data)
            return decoded[0]  # Return the array itself

    def get_user_reserve_data(
        self, user_asset_pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Dict]:
        """
        Issues a batch of getUserReserveData(address reserve, address user) calls.
        Returns a dict mapping (user, asset) tuple to result containing collateral status.

        Args:
            user_asset_pairs: List of (user_address, asset_address) tuples

        Returns:
            Dictionary mapping (user, asset) to decoded result with usageAsCollateralEnabled field
        """
        call_targets = [
            {
                "method_signature": "getUserReserveData(address,address)",
                "param_types": ["address", "address"],
                "params": [asset, user],  # Note: reserve (asset) comes first, then user
            }
            for user, asset in user_asset_pairs
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            return self.decode_eth_call_result(hex_result, "getUserReserveData")

        results_by_pair = {}
        if isinstance(raw_results, list):
            for (user, asset), rpc_result in zip(user_asset_pairs, raw_results):
                result_value = rpc_result.get("result", "")
                results_by_pair[(user, asset)] = decode_result(result_value)
            return results_by_pair
        return raw_results
