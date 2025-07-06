import logging

from django.core.cache import cache
from django.core.management.base import BaseCommand

from blockchains.tasks import ParentSynchronizeTask, UpdateNetworkBlockInfoTask
from oracles.management.commands.listen_base import WebsocketCommand
from oracles.tasks import PriceEventDynamicSynchronizeTask
from utils.constants import NETWORK_NAME

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class Command(WebsocketCommand, BaseCommand):
    help = "Subscribe to new blocks on the Ethereum blockchain using websockets"

    def get_subscribe_message(self):
        message = {
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
        }
        logger.debug(f"Subscribe message created: {message}")
        return message

    async def process(self, response):
        """
        Process incoming block data and cache the latest block number.
        """
        try:
            # Extract block data from the response
            if "params" not in response or "result" not in response["params"]:
                logger.warning(f"Invalid response structure: {response}")
                return

            block_data = response["params"]["result"]

            # Extract block information
            block_number_hex = block_data.get("number")
            block_hash = block_data.get("hash")
            block_timestamp_hex = block_data.get("timestamp")

            if not block_number_hex:
                logger.warning(f"No block number in response: {block_data}")
                return

            # Convert hex to decimal
            block_number = int(block_number_hex, 16)
            block_timestamp = (
                int(block_timestamp_hex, 16) if block_timestamp_hex else None
            )

            # Cache the block number using the same key as cached_block_height
            cache_key = f"block_height_{NETWORK_NAME}"
            cache.set(cache_key, block_number)

            logger.info(
                f"Block {block_number} received and cached "
                f"(hash: {block_hash}, timestamp: {block_timestamp})"
            )

            # Update ClickHouse NetworkBlockInfo table
            if block_timestamp:
                try:
                    UpdateNetworkBlockInfoTask.delay(block_number, block_timestamp)
                    logger.debug(
                        f"Queued NetworkBlockInfo update task for block {block_number}"
                    )
                except Exception as e:
                    logger.error(f"Failed to queue NetworkBlockInfo update task: {e}")

            ParentSynchronizeTask.delay()
            PriceEventDynamicSynchronizeTask.delay()

        except Exception as e:
            logger.error(f"Error processing block data: {e}")
            logger.error(f"Response: {response}")
