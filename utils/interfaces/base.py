import json
from typing import Any, Dict, List, Sequence

import requests
from decouple import config
from eth_abi import decode, encode
from eth_utils import keccak

from oracles.contracts.utils import RpcCacheStorage


class BaseContractInterface:
    """
    Base class for contract interfaces providing common functionality for
    batch RPC calls, ABI management, and result decoding.
    """

    def __init__(self, contract_address: str):
        """
        Initialize the interface with a contract address.

        Args:
            contract_address: The Ethereum address of the contract
        """
        self.contract_address = contract_address
        self.rpc_url = config("NETWORK_RPC")

        # Fetch and cache the ABI using RpcCacheStorage
        # get_contract_info returns (name, abi), we just want abi as a list/dict
        _, abi = RpcCacheStorage.get_contract_info(self.contract_address)
        # ABIs may be dumped as JSON strings, so parse if needed
        if isinstance(abi, str):
            self.abi = json.loads(abi)
        else:
            self.abi = abi

    def _make_batch_request(self, batch: List[dict]) -> Any:
        """
        Send a batch of RPC requests to the node.

        Args:
            batch: List of JSON-RPC request objects

        Returns:
            Response from the RPC node

        Raises:
            RuntimeError: If the batch request fails
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
        """
        Encodes a function call into calldata.

        Args:
            method_signature: Function signature (e.g., "getUserEMode(address)")
            param_types: List of parameter types
            params: List of parameter values

        Returns:
            Hex-encoded calldata string
        """
        selector = keccak(text=method_signature)[:4]
        encoded_params = encode(param_types, params)
        calldata = b"".join([selector, encoded_params])
        return "0x" + calldata.hex()

    def prepare_eth_call_batch(self, call_targets: List[dict]) -> List[dict]:
        """
        Prepares a batch of eth_call requests.

        Args:
            call_targets: List of call target dictionaries containing:
                - method_signature: Function signature string
                - param_types: List of parameter types
                - params: List of parameter values
                - to (optional): Override contract address

        Returns:
            List of JSON-RPC request objects
        """
        batch = []
        for i, call in enumerate(call_targets):
            to_addr = call.get("to", self.contract_address)
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
        """
        Executes a batch of eth_call requests.

        Args:
            call_targets: List of call target dictionaries

        Returns:
            List of RPC responses
        """
        batch = self.prepare_eth_call_batch(call_targets)
        return self._make_batch_request(batch)

    def get_abi_method(self, method_name: str) -> Dict:
        """
        Returns the ABI entry for a method by name.

        Args:
            method_name: Name of the function to look up

        Returns:
            ABI entry dictionary for the method

        Raises:
            ValueError: If the method is not found in the ABI
        """
        for entry in self.abi:
            if entry.get("type") == "function" and entry.get("name") == method_name:
                return entry
        raise ValueError(f"ABI: Method {method_name} not found.")

    def decode_eth_call_result(self, hex_result: str, method_name: str) -> Any:
        """
        Decodes the output of an eth_call using the relevant ABI entry.

        Args:
            hex_result: Hex-encoded result from eth_call
            method_name: Name of the method to get output types from

        Returns:
            Decoded result (single value or dictionary for multiple outputs)
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
