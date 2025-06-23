import logging
from typing import Dict, List, Optional

import urllib3
from decouple import config
from django.core.cache import cache
from web3 import Web3
from web3.middleware import validation

from utils.constants import (
    NETWORK_BLOCK_TIME,
    NETWORK_NAME,
    NETWORK_REFERENCE_BLOCK,
    NETWORK_REFERENCE_TIMESTAMP,
)

# This disables all method validations, including chainId checks
validation.METHODS_TO_VALIDATE = []

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logger = logging.getLogger(__name__)


def get_evm_block_timestamps(blocks: List[int]) -> Dict:
    return {
        int(block): NETWORK_REFERENCE_TIMESTAMP
        + (block - NETWORK_REFERENCE_BLOCK) * NETWORK_BLOCK_TIME
        for block in blocks
    }


class EVMRpcAdapter:
    def __init__(self) -> None:
        self.rpc_url = config("NETWORK_RPC")
        self.client = Web3(
            provider=Web3.HTTPProvider(
                endpoint_uri=self.rpc_url,
                request_kwargs={"timeout": 15, "verify": False},
            )
        )

    @property
    def block_height(self):
        """
        Gets the current block height of the network.

        Returns:
            int: The current block number.
        """
        return self.client.eth.block_number

    @property
    def cached_block_height(self):
        cached_block_height = cache.get(f"block_height_{NETWORK_NAME}")
        if cached_block_height is None:
            cached_block_height = self.block_height
            cache.set(f"block_height_{NETWORK_NAME}", cached_block_height, timeout=5)
        return cached_block_height

    @property
    def max_blockrange_size_for_events(self) -> int:
        return config("MAX_BLOCKRANGE_SIZE_FOR_EVENTS", cast=int, default=1_000_000)

    def get_raw_transaction(self, transaction_id: str) -> Dict:
        """
        Get the raw transaction data from the network using the transaction ID.

        Args:
            transaction_id (str): The transaction ID.

        Returns:
            Dict: The raw transaction data.
        """
        return self.client.eth.get_transaction(transaction_hash=transaction_id)

    def extract_raw_event_data(
        self,
        topics: List[str],
        contract_addresses: List[str],
        start_block: int,
        end_block: int,
    ) -> Optional[List[Dict]]:
        """
        Extract data for the given event signature from the blocknetwork.

        Args:
            signature (str): The event signature.
            start_block (int): The starting block number for the event logs extraction.
            end_block (int): The ending block number for the event logs extraction.

        Returns:
            Optional[List[Dict]]: Raw event logs for the event signature, or None if an error occurs.
        """
        return self.client.eth.get_logs(
            {
                "fromBlock": hex(start_block),
                "toBlock": hex(end_block),
                "topics": [
                    topics,
                ],
                "address": contract_addresses,
            }
        )

    def get_bytecode(self, address: str) -> str:
        """
        Get the bytecode deployed at a contract address.

        Args:
            address (str): The contract address to get bytecode from.

        Returns:
            str: The bytecode at the given address as a hex string.
        """
        return self.client.eth.get_code(address).hex()


rpc_adapter = EVMRpcAdapter()
