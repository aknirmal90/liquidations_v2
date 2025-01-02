import logging
from datetime import datetime, timezone

from django.core.management.base import BaseCommand

from aave.management.commands.listen_base import WebsocketCommand
from aave.tasks import UpdateAssetPriceTask

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Command(WebsocketCommand, BaseCommand):
    help = "Subscribe to new pending transactions on a blockchain using websockets"

    def get_subscribe_message(self):
        message = {
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": self.contract_addresses,
                    "topics": [
                        "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
                    ],
                }
            ],
        }
        logger.debug(f"Subscribe message created: {message}")
        return message

    def parse_log(self, log):
        return {
            "asset": log["address"],
            "new_price": int(log["topics"][1], 16),
            "block_height": int(log["blockNumber"], 16),
            "updated_at": int(log["data"], 16),
            "roundId": int(log["topics"][2], 16),
            "transaction_hash": log["transactionHash"],
        }

    async def process(self, msg, **kwargs):
        onchain_received_at = datetime.now(timezone.utc)

        if not ("params" in msg and "result" in msg["params"]):
            return

        log = msg["params"]["result"]
        parsed_log = self.parse_log(log)
        is_new_price = self.check_and_update_price_cache(
            new_price=parsed_log["new_price"],
            asset=parsed_log["asset"]
        )
        if not is_new_price:
            return

        processed_at = datetime.now(timezone.utc)

        UpdateAssetPriceTask.apply_async(
            kwargs={
                "network_id": self.network.id,
                "network_name": self.network.name,
                "contract": parsed_log['asset'],  # Use already lowercased value
                "new_price": parsed_log['new_price'],
                "onchain_created_at": parsed_log['updated_at'],
                "round_id": parsed_log['roundId'],
                "onchain_received_at": onchain_received_at,
                "provider": self.provider,
                "transaction_hash": parsed_log['transaction_hash'],
                "processed_at": processed_at
            },
            priority=0  # High priority
        )
