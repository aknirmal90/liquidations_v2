import asyncio
import logging
import time

import orjson as json
import websockets
from django.core.cache import cache
from django.core.management.base import BaseCommand

from oracles.management.commands.listen_base import WebsocketCommand
from oracles.tasks import RecordTransactionTimingTask
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
    help = "Subscribe to mempool transactions on QuickNode using websockets"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.total_transactions = 0
        self.filtered_transactions = 0
        self.processed_transactions = 0

    def get_subscribe_message(self):
        """
        Create subscription message for QuickNode mempool transactions.
        QuickNode uses eth_subscribe with "newPendingTransactions" method.
        Setting includeTransactions to true to get full transaction payloads.
        """
        message = {
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newPendingTransactions", True],
        }
        logger.debug(f"Subscribe message created: {message}")
        return message

    async def listen(self):
        """
        Override the listen method to use QuickNode WebSocket endpoint.
        """
        wss = "wss://distinguished-chaotic-field.quiknode.pro/72950ba7ca289f717c9fecd5a8ff574d94ed7eb0"
        logger.info(f"Connecting to QuickNode mempool WebSocket: {wss}")

        # Pre-compute subscribe message once
        subscribe_message = self.get_subscribe_message()
        subscribe_message_json = json.dumps(subscribe_message)
        logger.info(f"Subscription message: {subscribe_message_json}")

        # Reconnection loop
        while True:
            try:
                # Attempt to connect with optimized settings
                async with websockets.connect(
                    wss,
                    ping_interval=None,  # Disable ping/pong
                    max_size=2**24,  # Increase max message size
                    compression=None,  # Disable compression
                ) as websocket:
                    await websocket.send(subscribe_message_json)
                    logger.info("Successfully subscribed to QuickNode mempool")

                    # Process messages as fast as possible
                    while True:
                        msg = json.loads(await websocket.recv())
                        if "params" not in msg:
                            continue

                        # Process message without awaiting to reduce latency
                        asyncio.create_task(self.process(msg))

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed with error: {e}. Reconnecting...")
                await asyncio.sleep(0.1)  # Reduced reconnection delay
            except Exception as e:
                logger.error(f"Error in connection: {e}")
                break

    async def process(self, response):
        """
        Process incoming mempool transaction data with filtering and oracle processing.
        """
        try:
            # Extract transaction data from the response
            if "params" not in response or "result" not in response["params"]:
                return

            tx_data = response["params"]["result"]
            self.total_transactions += 1

            # Log every 100 transactions for monitoring
            if self.total_transactions % 100 == 0:
                logger.info(
                    f"Processed {self.total_transactions} transactions, "
                    f"filtered {self.filtered_transactions}, "
                    f"successfully processed {self.processed_transactions}"
                )

            # Filter for relevant transactions
            if not self._is_relevant_transaction(tx_data):
                return

            self.filtered_transactions += 1
            logger.info(
                f"Processing relevant transaction: {tx_data.get('hash', 'unknown')}"
            )

            # Check if this is a forwarder call
            if "input" in tx_data and tx_data["input"].startswith("0x6fadcf72"):
                logger.info(f"Processing forwarder call: {tx_data['hash']}")

                try:
                    # Parse the forwarder call
                    parsed_data = parse_forwarder_call(tx_data["input"])

                    # Record unconfirmed transaction timing with oracle address
                    tx_hash = tx_data.get("hash")
                    oracle_address = parsed_data.get("oracle_address")
                    if tx_hash and oracle_address:
                        asyncio.create_task(
                            self._record_unconfirmed_transaction(
                                tx_hash, oracle_address
                            )
                        )

                    await self.handle_oracle_update(parsed_data, tx_data)
                    self.processed_transactions += 1

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
            logger.error(f"Error processing mempool transaction: {e}")

    def _is_relevant_transaction(self, tx_data):
        """
        Filter transactions to only process those relevant to oracle updates.

        Args:
            tx_data: Transaction data from mempool

        Returns:
            bool: True if transaction is relevant for oracle processing
        """
        try:
            # Get cached transmitters and authorized senders
            allowed_transmitters = cache.get("transmitters_for_websockets", [])
            authorized_senders = cache.get("authorized_senders_for_websockets", [])

            if not allowed_transmitters or not authorized_senders:
                logger.warning("No transmitters or authorized senders in cache")
                return False

            # Must have input data (for forwarder calls)
            if not tx_data.get("input") or len(tx_data["input"]) < 10:
                return False

            # Check if transaction is to a known transmitter
            to_address = tx_data.get("to", "").lower()
            # Must be to a known transmitter
            if to_address not in allowed_transmitters:
                return False

            from_address = tx_data.get("from", "").lower()
            # Must be from an authorized sender
            if from_address not in authorized_senders:
                return False

            return True

        except Exception as e:
            logger.error(f"Error filtering transaction: {e}. {tx_data}")
            return False

    async def _record_unconfirmed_transaction(self, tx_hash: str, oracle_address: str):
        """
        Record the unconfirmed transaction timestamp in ClickHouse.

        Args:
            tx_hash: The transaction hash
            oracle_address: The oracle address from the parsed transaction
        """
        try:
            unconfirmed_ts = int(time.time())

            # Get asset sources for this oracle address
            asset_sources = cache.get(f"underlying_asset_source_{oracle_address}")

            if asset_sources:
                # Record timing for each asset source
                for asset, asset_source in asset_sources:
                    RecordTransactionTimingTask.delay(
                        tx_hash=tx_hash,
                        asset_source=asset_source,
                        unconfirmed_tx_ts=unconfirmed_ts,
                        confirmed_tx_ts=0,
                        mev_share_ts=0,
                    )
                    logger.debug(
                        f"Recorded unconfirmed tx {tx_hash} for asset_source {asset_source}"
                    )
            else:
                logger.warning(
                    f"No asset sources found for oracle {oracle_address}, skipping timing record"
                )
        except Exception as e:
            logger.error(f"Error recording unconfirmed transaction {tx_hash}: {e}")
