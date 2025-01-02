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
    # The first byte is often the 'kind', but if it's a final transaction,
    # we skip it to decode the actual Ethereum RLP data.
    # If raw_l2_bytes[0] == 3, 4, 5, etc., you might need to handle offsets.
    # However, in our approach, parse_l2_message returns the entire chunk
    # (including the kind). Let's do a small check:
    message_kind = raw_l2_bytes[0]

    # If it's a batch kind or something else, skip decoding
    if message_kind == ArbitrumL2Parser.L2MESSAGE_KIND_BATCH:
        # This shouldn't happen here because we parse batches recursively,
        # but let's just be safe.
        logger.debug("Encountered a batch kind in recover_transaction (unexpected).")
        return None

    # Otherwise, treat the entire chunk minus the first byte as the RLP/tx payload
    eth_tx_data = raw_l2_bytes[1:]

    # Attempt typed-transaction decode
    try:
        tx = TypedTransaction.from_bytes(HexBytes(eth_tx_data)).as_dict()
        tx['hash'] = "0x" + Web3.keccak(eth_tx_data).hex()
        return tx
    except Exception:
        pass

    # Attempt legacy decode
    try:
        tx_legacy = legacy_transactions.Transaction.from_bytes(HexBytes(eth_tx_data)).as_dict()
        tx_legacy['hash'] = "0x" + Web3.keccak(eth_tx_data).hex()
        return tx_legacy
    except Exception as e:
        logger.error(f"Error decoding transaction: {e}")
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
                # Connect to websocket
                self.websocket = await websockets.connect(
                    uri="wss://arb1.arbitrum.io/feed",
                    ping_timeout=None
                )
                logger.info("Connected to Arbitrum sequencer feed")

                # Listen for messages
                while True:
                    raw_msg = await self.websocket.recv()
                    msg = json.loads(raw_msg)
                    await self.process(msg)

            except Exception as e:
                logger.error(f"Error processing sequencer message: {e}")
                continue

    async def process(self, msg, **kwargs):
        """Process messages from the sequencer feed."""
        # 'messages' is a list of objects each containing a 'message' with 'l2Msg'
        try:
            messages = msg["messages"]
        except KeyError:
            return

        collected_txs = []
        onchain_received_at = datetime.now(timezone.utc)
        for message in messages:
            # 'message["message"]["message"]' => the actual payload
            inner_message = message["message"]["message"]
            # base64 decode the 'l2Msg'
            l2_message_b64 = inner_message["l2Msg"]
            l2_message = base64.b64decode(l2_message_b64)

            # Recursively parse L2 message into final transaction chunks
            final_chunks = ArbitrumL2Parser.parse_l2_message(l2_message, depth=0)

            # Decode each chunk
            for chunk in final_chunks:
                tx_dict = recover_transaction(chunk)
                if tx_dict is not None:
                    collected_txs.append(tx_dict)

        for tx in collected_txs:
            receiver = add_0x_prefix(tx["to"])
            input_data = add_0x_prefix(tx["data"])

            if receiver in self.contract_addresses:
                latest_answer = get_latest_answer(input_data)
                is_new_price = self.check_and_update_price_cache(
                    new_price=latest_answer['median'],
                    asset=receiver
                )
                if not is_new_price:
                    return

                logger.info(f"Latest answer: {latest_answer}")

                UpdateAssetPriceTask.apply_async(
                    kwargs={
                        "network_id": self.network.id,
                        "contract": receiver,  # Use already lowercased value
                        "new_price": latest_answer['median'],
                        "onchain_received_at": onchain_received_at,
                        "provider": self.provider,
                        "transaction_hash": tx['hash']
                    },
                    priority=0  # High priority
                )
