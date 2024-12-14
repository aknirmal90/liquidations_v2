import logging


logger = logging.getLogger(__name__)


class EvmTokenRetriever:
    # Define the ABI as a class constant
    ABI = [
        {
            "inputs": [],
            "name": "name",
            "outputs": [{"internalType": "string", "name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "symbol",
            "outputs": [{"internalType": "string", "name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function"
        }
    ]

    def __init__(self, network_name: str, token_address: str):
        # Initialize Web3 connection
        self.adapter = self.network.rpc_adapter

        # Initialize the contract object using the ABI and the contract address
        self.contract_caller = self.adapter.client.eth.contract(
            address=Web3.to_checksum_address(
                decode_to_hex_address(
                    network_name=self.network.network_name,
                    token_address=token_address
                )
            ),
            abi=self.ABI
        )
        self.token_address = encode_to_native_address(
            network_name=self.network.network_name,
            wallet_address=token_address
        )

    @property
    def name(self):
        """Retrieve the token name, first checking Redis."""
        try:
            return self.contract_caller.functions.name().call()
        except Exception as e:
            logger.error(f"Failed to retrieve token name: {e}")
            return None

    @property
    def symbol(self):
        """Retrieve the token symbol, first checking Redis."""
        try:
            return self.contract_caller.functions.symbol().call()
        except Exception as e:
            logger.error(f"Failed to retrieve token symbol: {e}")
            return None

    @property
    def decimals(self):
        """Retrieve the token decimals, first checking Redis."""
        try:
            return int(self.contract_caller.functions.decimals().call())
        except Exception as e:
            logger.error(f"Failed to retrieve token symbol: {e}")
            return None
