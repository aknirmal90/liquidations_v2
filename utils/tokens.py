import logging

from web3 import Web3

logger = logging.getLogger(__name__)


class EvmTokenRetriever:
    # Define the ABI as a class constant
    SECONDARY_NAME_ABI = [{
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [
            {
                "name": "",
                "type": "bytes32"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },]

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
            "name": "underlying",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
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
        # Retrieve the RPC URL from the network name using the utility function
        from blockchains.models import Network
        self.network = Network.get_network_by_name(network_name)

        # Initialize Web3 connection
        self.adapter = self.network.rpc_adapter

        # Initialize the contract object using the ABI and the contract address
        self.contract_caller = self.adapter.client.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=self.ABI
        )
        self.secondary_contract_caller = self.adapter.client.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=self.SECONDARY_NAME_ABI
        )

        self.token_address = token_address.lower()

    @property
    def name(self):
        """Retrieve the token name, first checking Redis."""
        try:
            return self.contract_caller.functions.name().call().rstrip('\x00')
        except Exception as e:
            logger.error(f"Failed to retrieve token name with primary contract caller: {e}")
            try:
                name = self.secondary_contract_caller.functions.name().call()
                logger.info("Successfully retrieved token name with secondary contract caller.")
                return name.decode("utf-8").rstrip('\x00')
            except Exception as e:
                logger.error(f"Failed to retrieve token name with secondary contract caller: {e}")
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
    def num_decimals(self):
        """Retrieve the token decimals, first checking Redis."""
        try:
            return int(self.contract_caller.functions.decimals().call())
        except Exception as e:
            logger.error(f"Failed to retrieve token symbol: {e}")
            return None
