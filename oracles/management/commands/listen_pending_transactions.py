import logging

from django.core.cache import cache
from django.core.management.base import BaseCommand

from oracles.management.commands.listen_base import WebsocketCommand
from oracles.models import PriceEvent
from utils.constants import NETWORK_NAME, PROTOCOL_NAME
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
        transmitters = cache.get(
            f"price_events_transmitters_{PROTOCOL_NAME}_{NETWORK_NAME}"
        )
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

    async def handle_oracle_update(self, parsed_data: dict, tx_data: dict):
        """
        Handle oracle price updates and make liquidation decisions.

        Args:
            parsed_data: Parsed oracle data from forwarder call
            tx_data: Original transaction data
        """
        median_price = parsed_data["median_price"]
        oracle_address = parsed_data["oracle_address"]
        # epoch_and_round = parsed_data["epoch_and_round"]

        logger.info(
            f"Processing oracle update for {oracle_address} with median price: {median_price}"
        )

        price_events = PriceEvent.objects.filter(
            contract_addresses__contains=oracle_address
        )
        for price_event in price_events.iterator():
            # contract_interface = price_event.contract_interface
            # numerator = contract_interface.get_numerator(median_price)
            if price_event.transmitters:
                for transmitter in price_event.transmitters:
                    logger.info(
                        f"Transmitting price for {transmitter} with median price: {median_price}"
                    )

        if median_price > 0:
            logger.info(
                f"✅ Price is positive ({median_price}), proceeding with liquidation logic"
            )
            # TODO: Implement your liquidation decision logic here
        else:
            logger.warning(
                f"⚠️ Price is non-positive ({median_price}), skipping liquidation"
            )
