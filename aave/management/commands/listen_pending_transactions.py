import logging

from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand

from aave.management.commands.listen_base import WebsocketCommand
from aave.models import Asset

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
        }

    async def process(self, msg, **kwargs):
        if not ("params" in msg and "result" in msg["params"]):
            return

        log = msg["params"]["result"]
        parsed_log = self.parse_log(log)
        logger.info(parsed_log)

        await sync_to_async(self.update_asset_price, thread_sensitive=True)(
            contract=parsed_log["asset"],
            new_price=parsed_log["new_price"],
            block_height=parsed_log["block_height"],
        )

    def update_asset_price(self, contract, new_price, block_height):
        logger.debug(f"Updating price for contract {contract} to {new_price} at block {block_height}")

        self.network.refresh_from_db()
        assets = Asset.objects.filter(contractA__iexact=contract)

        if assets.count() > 0:
            logger.debug(f"Found {assets.count()} assets with contractA matching {contract}")
            assets.update(
                priceA=new_price, updated_at_block_heightA=block_height
            )
            # for asset in assets:
            # asset._set_price()
            logger.info(f"Asset {contract} Price updated for {new_price} on ContractA")
        else:
            logger.error(f"Asset {contract} not found on ContractA. Price not updated.")

        assets = Asset.objects.filter(contractB__iexact=contract)
        if assets.count() > 0:
            logger.debug(f"Found {assets.count()} assets with contractB matching {contract}")
            assets.update(
                priceB=new_price, updated_at_block_heightB=block_height
            )
            # for asset in assets:
            # asset._set_price()
            logger.info(f"Asset {contract} Price updated for {new_price} on ContractB")
        else:
            logger.error(f"Asset {contract} not found on ContractB. Price not updated.")
