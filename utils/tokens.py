import logging

from web3 import Web3

from utils.rpc import rpc_adapter

logger = logging.getLogger(__name__)


class Token:
    def __init__(self, asset: str):
        self.asset = asset

    def call_function(self, function_name: str, *args, **kwargs):
        if not hasattr(self, "contract"):
            self.contract = rpc_adapter.client.eth.contract(
                address=Web3.to_checksum_address(self.asset), abi=self.abi
            )

        logger.info(
            f"Calling function {function_name} with args {args} and kwargs {kwargs}"
        )
        return self.contract.functions[function_name](*args, **kwargs).call()

    def decimals(self):
        try:
            return self.call_function("decimals")
        except Exception as e:
            try:
                return self.call_function("DECIMALS")
            except Exception as e:
                return self.call_function("RATIO_DECIMALS")

    @property
    def abi(self):
        return [
            {
                "inputs": [],
                "name": "DECIMALS",
                "outputs": [
                    {
                        "internalType": "uint8",
                        "name": "",
                        "type": "uint8"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "decimals",
                "outputs": [
                    {
                        "internalType": "uint8",
                        "name": "",
                        "type": "uint8"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "RATIO_DECIMALS",
                "outputs": [
                    {
                        "internalType": "uint8",
                        "name": "",
                        "type": "uint8"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
