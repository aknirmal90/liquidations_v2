import logging

from django.core.cache import cache
from django.core.management.base import BaseCommand

from oracles.management.commands.listen_base import WebsocketCommand
from utils.oracle import (
    InvalidMethodSignature,
    InvalidObservations,
    parse_forwarder_call,
)

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class Command(WebsocketCommand, BaseCommand):
    help = "Subscribe to new pending transactions on a blockchain using websockets"

    def get_subscribe_message(self):
        transmitters = cache.get("transmitters_for_websockets")
        message = {
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": [
                "alchemy_pendingTransactions",
                {
                    "toAddress": transmitters,
                },
            ],
        }
        logger.debug(f"Subscribe message created: {message}")
        return message

    async def process(self, response):
        """
        Process incoming pending transaction data and parse forwarder calls.
        """
        try:
            # Extract transaction data from the response
            if "params" not in response or "result" not in response["params"]:
                return

            tx_data = response["params"]["result"]

            logger.info(f"Processing pending transaction: {tx_data}")

            # Check if this is a forwarder call
            if "input" in tx_data and tx_data["input"].startswith("0x6fadcf72"):
                logger.info(f"Processing forwarder call: {tx_data['hash']}")

                try:
                    # Parse the forwarder call
                    parsed_data = parse_forwarder_call(tx_data["input"])
                    await self.handle_oracle_update(parsed_data, tx_data)

                except InvalidMethodSignature as e:
                    logger.warning(
                        f"Invalid method signature in transaction {tx_data['hash']}: {e}"
                    )
                except InvalidObservations as e:
                    logger.warning(
                        f"Invalid observations in transaction {tx_data['hash']}: {e}"
                    )
                except Exception as e:
                    logger.error(f"Error parsing forwarder call {tx_data['hash']}: {e}")

        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
