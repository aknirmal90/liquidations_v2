import logging
from datetime import datetime, timezone

from django.core.cache import cache
from django.core.management.base import BaseCommand

from aave.management.commands.listen_base import WebsocketCommand
from aave.models import Asset
from aave.tasks import UpdateAssetPriceTask

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Command(WebsocketCommand, BaseCommand):
    help = "Subscribe to new pending transactions on a blockchain using websockets"

    def get_subscribe_message(self):
        logger.debug("Getting subscribe message")
        chainlink_assets = Asset.objects.filter(network=self.network)
        chainlink_assets_contractA = list(
            chainlink_assets.values_list("contractA", flat=True)
        )
        chainlink_assets_contractB = list(
            chainlink_assets.values_list("contractB", flat=True)
        )
        chainlink_assets_contracts = [
            contract
            for contract in list(
                set(chainlink_assets_contractA + chainlink_assets_contractB)
            )
            if contract
        ]
        logger.debug(f"Found {len(chainlink_assets_contracts)} contracts to monitor")

        message = {
            "id": "1",
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": chainlink_assets_contracts,
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
        }

    async def process(self, msg, **kwargs):
        if not ("params" in msg and "result" in msg["params"]):
            return

        onchain_received_at = datetime.now(timezone.utc)

        log = msg["params"]["result"]
        parsed_log = self.parse_log(log)

        # Get cached price from Django cache
        cache_key = f"price-{self.network_name}-{parsed_log['asset'].lower()}"
        cached_price = cache.get(cache_key)

        # Skip update if price hasn't changed
        if cached_price == parsed_log["new_price"]:
            return

        # Update cache with new price
        cache.set(cache_key, parsed_log["new_price"])

        # Fire task to update asset price
        UpdateAssetPriceTask.delay(
            network_id=self.network.id,
            contract=parsed_log['asset'],
            new_price=parsed_log['new_price'],
            block_height=parsed_log['block_height'],
            onchain_created_at=parsed_log['updated_at'],
            round_id=parsed_log['roundId'],
            onchain_received_at=onchain_received_at
        )
