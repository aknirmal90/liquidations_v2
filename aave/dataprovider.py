from typing import List

import requests
import web3

from blockchains.models import Network, Protocol
from config.models import Configuration
from utils.encoding import add_0x_prefix, decode_any, get_decoded_params, get_encoded_params


class AaveDataProvider:
    def __init__(self, network_name: str):
        self.network = Network.objects.get(name=network_name)
        self.data_provider_contract_address = Configuration.get(f"AAVE_DATA_PROVIDER_{self.network.chain_id}")
        self.price_oracle_contract_address = Configuration.get(f"AAVE_PRICE_ORACLE_{self.network.chain_id}")
        self.pool_contract_address = Configuration.get(f"AAVE_POOL_CONTRACT_{self.network.chain_id}")
        self.abi = Protocol.objects.get(name="aave").evm_abi

    def _make_payload(
        self,
        contract_name: str,
        function_name: str,
        params: list,
        id: int = 0,
        contract_address: str = None
    ):
        contract_address = contract_address or getattr(self, f"{contract_name}_contract_address")
        function_abi = web3.utils.filter_abi_by_name(function_name, self.abi)[0]
        encoded_params = get_encoded_params(function_abi, params)
        function_signature = web3.utils.abi_to_signature(function_abi)
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": web3.Web3.to_checksum_address(contract_address),
                    "data": add_0x_prefix(web3.Web3.keccak(text=function_signature).hex()[:8] + encoded_params)
                },
                "latest"
            ],
            "id": id
        }
        return payload

    def get_batch_response(
        self,
        contract_name: str,
        function_name: str,
        params_list: list,
        contract_address: str = None
    ):
        function_abi = web3.utils.filter_abi_by_name(function_name, self.abi)[0]
        payloads = [
            self._make_payload(
                contract_name,
                function_name,
                params,
                i,
                contract_address
            ) for i, params in enumerate(params_list)
        ]
        responses = requests.post(
            self.network.rpc,
            json=payloads
        )
        return [
            {
                "id": response.get("id"),
                "result": decode_any(get_decoded_params(function_abi, response.get("result"))),
            } for response in responses.json()
        ]

    def getUserReserveData(self, reserve: str, users: List[str]):
        return self.get_batch_response(
            contract_name="data_provider",
            function_name="getUserReserveData",
            params_list=[
                [reserve, user] for user in users
            ]
        )

    def getReserveConfigurationData(self, reserves: List[str]):
        return self.get_batch_response(
            contract_name="data_provider",
            function_name="getReserveConfigurationData",
            params_list=[
                [reserve] for reserve in reserves
            ]
        )

    def getReserveTokensAddresses(self, reserves: List[str]):
        return self.get_batch_response(
            contract_name="data_provider",
            function_name="getReserveTokensAddresses",
            params_list=[
                [reserve] for reserve in reserves
            ]
        )

    def getSourceOfAsset(self, assets: List[str]):
        return self.get_batch_response(
            contract_name="price_oracle",
            function_name="getSourceOfAsset",
            params_list=[
                [asset] for asset in assets
            ]
        )

    def getPreviousIndex(self, contract_address: str, users: List[str]):
        return self.get_batch_response(
            contract_name=None,
            contract_address=contract_address,
            function_name="getPreviousIndex",
            params_list=[
                [user] for user in users
            ]
        )

    def getUserEMode(self, users: List[str]):
        return self.get_batch_response(
            contract_name="pool",
            function_name="getUserEMode",
            params_list=[
                [user] for user in users
            ]
        )
