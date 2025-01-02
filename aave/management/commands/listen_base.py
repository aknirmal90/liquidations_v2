import asyncio
import logging

import orjson as json
import websockets
from asgiref.sync import sync_to_async
from django.core.cache import cache

from aave.models import Asset
from blockchains.models import Network

logger = logging.getLogger(__name__)


class WebsocketCommand:
    help = "Subscribe to new blocks on the Ethereum blockchain using websockets"

    def add_arguments(self, parser):
        parser.add_argument(
            "--network",
            action="store",
            help="Network name as stored on related model instance",
        )
        parser.add_argument(
            "--provider",
            action="store",
            help="Provider to use for websocket connection (infura, alchemy, quicknode, nodereal)",
        )

    async def get_network(self, network_id):
        instance = await sync_to_async(Network.objects.get, thread_sensitive=True)(
            name=network_id
        )
        return instance

    async def listen(self):
        wss = getattr(self.network, f"wss_{self.provider}")
        logger.info(f"Connecting to {wss}")

        # Pre-compute subscribe message once
        subscribe_message = await sync_to_async(
            self.get_subscribe_message, thread_sensitive=True
        )()
        subscribe_message_json = json.dumps(subscribe_message)
        logger.info(subscribe_message_json)

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

    def handle(self, *args, **options):
        self.network_name = options["network"]
        self.network = Network.get_network(self.network_name)
        self.provider = options["provider"]
        self.contract_addresses = self.get_contract_addresses()

        # Use uvloop if available for better performance
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass

        asyncio.run(self.listen())

    def get_subscribe_message(self):
        raise NotImplementedError

    async def process(self, response):
        raise NotImplementedError

    def check_and_update_price_cache(self, new_price, asset):
        cache_key = f"price-{self.network_name}-{self.provider}-{asset}"
        cached_price = cache.get(cache_key)

        if cached_price == new_price:
            return False

        cache.set(cache_key, new_price)
        return True

    def get_contract_addresses(self):
        """Get contract addresses to monitor from Asset model."""
        chainlink_assets = Asset.objects.filter(network=self.network)
        chainlink_assets_contractA = list(
            chainlink_assets.values_list("contractA", flat=True)
        )
        chainlink_assets_contractB = list(
            chainlink_assets.values_list("contractB", flat=True)
        )
        return [
            contract
            for contract in list(
                set(chainlink_assets_contractA + chainlink_assets_contractB)
            )
            if contract
        ]
