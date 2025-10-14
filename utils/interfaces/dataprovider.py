import json
from typing import Any, Dict, List, Sequence

import requests
from decouple import config
from eth_abi import decode, encode
from eth_utils import keccak

from oracles.contracts.utils import RpcCacheStorage


class DataProviderInterface:
    def __init__(self):
        self.data_provider_address = config("POOL_V3_DATAPROVIDER")
        self.rpc_url = config("NETWORK_RPC")

        # Fetch and cache the ABI using RpcCacheStorage
        # get_contract_info returns (name, abi), we just want abi as a list/dict
        _, abi = RpcCacheStorage.get_contract_info(self.data_provider_address)
        # ABIs may be dumped as JSON strings, so parse if needed
        if isinstance(abi, str):
            self.abi = json.loads(abi)
        else:
            self.abi = abi

    def _make_batch_request(self, batch: List[dict]) -> Any:
        """
        Send a batch of RPC requests to the node.
        """
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(
                self.rpc_url,
                json=batch,
                headers=headers,
                timeout=20,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise RuntimeError(f"Batch RPC request failed: {e}")

    def _get_call_data(
        self, method_signature: str, param_types: Sequence[str], params: Sequence[Any]
    ) -> str:
        selector = keccak(text=method_signature)[:4]
        encoded_params = encode(param_types, params)
        calldata = b"".join([selector, encoded_params])
        return "0x" + calldata.hex()

    def prepare_eth_call_batch(self, call_targets: List[dict]) -> List[dict]:
        batch = []
        for i, call in enumerate(call_targets):
            to_addr = call.get("to", self.data_provider_address)
            calldata = self._get_call_data(
                call["method_signature"],
                call["param_types"],
                call["params"],
            )
            call_object = {
                "to": to_addr,
                "data": calldata,
            }
            batch.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [call_object, "latest"],
                    "id": i,
                }
            )
        return batch

    def batch_eth_call(self, call_targets: List[dict]) -> Any:
        batch = self.prepare_eth_call_batch(call_targets)
        return self._make_batch_request(batch)

    def get_abi_method(self, method_name: str) -> Dict:
        """
        Returns the ABI entry for a method by name.
        """
        for entry in self.abi:
            if entry.get("type") == "function" and entry.get("name") == method_name:
                return entry
        raise ValueError(f"ABI: Method {method_name} not found.")

    def decode_eth_call_result(self, hex_result: str, method_name: str) -> Any:
        """
        Decodes the output of an eth_call using the relevant ABI entry.
        """
        abi_entry = self.get_abi_method(method_name)
        output_types = [output["type"] for output in abi_entry.get("outputs", [])]
        data = bytes.fromhex(
            hex_result[2:] if hex_result.startswith("0x") else hex_result
        )
        decoded = decode(output_types, data)
        if len(decoded) == 1:
            return decoded[0]
        names = [
            item.get("name", f"ret{i}")
            for i, item in enumerate(abi_entry.get("outputs", []))
        ]
        return dict(zip(names, decoded))

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
