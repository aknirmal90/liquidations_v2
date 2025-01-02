import base64
import logging
from datetime import datetime, timezone

import orjson as json
import websockets
from django.core.management.base import BaseCommand
from eth_account._utils import legacy_transactions
from eth_account.datastructures import HexBytes
from eth_account.typed_transactions import TypedTransaction
from web3 import Web3

from aave.management.commands.listen_base import WebsocketCommand
from aave.tasks import UpdateAssetPriceTask
from utils.encoding import add_0x_prefix
from utils.oracle import get_latest_answer

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# -----------------------------------------
# Recursive L2 Message Parsing
# -----------------------------------------
class ArbitrumL2Parser:
    """
    A class containing static methods to parse L2 messages from the Arbitrum Sequencer feed.
    """

    L2MESSAGE_KIND_BATCH = 3  # L2MessageKind_Batch in your code
    MAX_DEPTH = 16

    @staticmethod
    def parse_l2_message(l2_data: bytes, depth: int = 0) -> list[bytes]:
        """
        Recursively parse an L2 message (including its kind byte) and return
        a list of 'final' transaction bytes that can be decoded as Ethereum
        (legacy or typed) transactions.
        """
        if not l2_data:
            return []

        if depth > ArbitrumL2Parser.MAX_DEPTH:
            logger.warning("Exceeded max batch depth - ignoring further nested messages")
            return []

        message_kind = l2_data[0]
        payload = l2_data[1:]  # everything after the 1-byte kind

        # If it's a batch kind, we parse sub-messages
        if message_kind == ArbitrumL2Parser.L2MESSAGE_KIND_BATCH:
            return ArbitrumL2Parser._parse_batch(payload, depth + 1)
        else:
            # For anything else (SignedTx, UnsignedTx, etc.), treat it as a single final transaction blob
            # The caller can then attempt to decode it with recover_transaction
            return [l2_data]

    @staticmethod
    def _parse_batch(batch_data: bytes, depth: int) -> list[bytes]:
        """
        Parse the contents of a batch. Each sub-message has a 4-byte length prefix
        (big-endian), followed by that many bytes of data. Then we recursively parse
        each sub-message in `parse_l2_message`.
        """
        offset = 0
        sub_txs = []

        while offset + 4 <= len(batch_data):
            # Read length prefix
            length_prefix = batch_data[offset : offset + 4]
            sub_len = int.from_bytes(length_prefix, 'big', signed=False)
            offset += 4

            # If there's not enough data left, break
            if offset + sub_len > len(batch_data):
                logger.warning("Truncated sub-message in batch.")
                break

            sub_msg = batch_data[offset : offset + sub_len]
            offset += sub_len

            # Recursively parse sub_msg (it has its own 'kind' byte at sub_msg[0])
            nested_list = ArbitrumL2Parser.parse_l2_message(sub_msg, depth)
            sub_txs.extend(nested_list)

        return sub_txs


# -----------------------------------------
# Transaction Decoding Logic
# -----------------------------------------
def recover_transaction(raw_l2_bytes: bytes) -> dict | None:
    """
    Attempt to decode a raw L2 message as a typed or legacy Ethereum transaction.
    Returns a dictionary of tx fields, or None if decoding fails.
    """
    message_kind = raw_l2_bytes[0]

    if message_kind == ArbitrumL2Parser.L2MESSAGE_KIND_BATCH:
        return None

    eth_tx_data = raw_l2_bytes[1:]

    # Try typed transaction first since it's more common
    try:
        tx = TypedTransaction.from_bytes(HexBytes(eth_tx_data)).as_dict()
        tx['hash'] = "0x" + Web3.keccak(eth_tx_data).hex()
        return tx
    except Exception:
        try:
            tx_legacy = legacy_transactions.Transaction.from_bytes(HexBytes(eth_tx_data)).as_dict()
            tx_legacy['hash'] = "0x" + Web3.keccak(eth_tx_data).hex()
            return tx_legacy
        except Exception:
            return None


# -----------------------------------------
# Django Management Command
# -----------------------------------------
class Command(WebsocketCommand, BaseCommand):
    help = "Subscribe to Arbitrum sequencer feed using websockets"

    async def listen(self):
        """Continuously listen to the sequencer feed."""
        while True:
            try:
                self.websocket = await websockets.connect(
                    uri="wss://arb1.arbitrum.io/feed",
                    ping_timeout=None,
                    compression=None  # Disable compression for lower latency
                )
                logger.info("Connected to Arbitrum sequencer feed")

                while True:
                    raw_msg = await self.websocket.recv()
                    msg = json.loads(raw_msg)
                    await self.process(msg)

            except Exception as e:
                logger.error(f"Error processing sequencer message: {e}")
                continue

    async def process(self, msg, **kwargs):
        """Process messages from the sequencer feed."""
        onchain_received_at = datetime.now(timezone.utc)

        try:
            messages = msg["messages"]
        except KeyError:
            return

        # Pre-allocate list with estimated size
        collected_txs = []
        for message in messages:
            inner_message = message["message"]["message"]
            l2_message = base64.b64decode(inner_message["l2Msg"])

            # Process chunks in parallel if possible
            final_chunks = ArbitrumL2Parser.parse_l2_message(l2_message, depth=0)

            for chunk in final_chunks:
                if tx_dict := recover_transaction(chunk):
                    collected_txs.append(tx_dict)

        # Process transactions in parallel if possible
        for tx in collected_txs:
            receiver = add_0x_prefix(tx["to"])

            # Skip if not a contract we care about
            if receiver not in self.contract_addresses:
                continue

            input_data = add_0x_prefix(tx["data"])
            latest_answer = get_latest_answer(input_data)

            # Skip if price hasn't changed
            if not self.check_and_update_price_cache(
                new_price=latest_answer['median'],
                asset=receiver
            ):
                continue

            logger.info(f"Latest answer: {latest_answer}")

            processed_at = datetime.now(timezone.utc)

            UpdateAssetPriceTask.apply_async(
                kwargs={
                    "network_id": self.network.id,
                    "network_name": self.network.name,
                    "contract": receiver,
                    "new_price": latest_answer['median'],
                    "onchain_received_at": onchain_received_at,
                    "provider": self.provider,
                    "transaction_hash": tx['hash'],
                    "processed_at": processed_at
                }
            )
