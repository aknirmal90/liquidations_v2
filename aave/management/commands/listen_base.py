import asyncio
import json
import logging

import websockets
from asgiref.sync import sync_to_async

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
        self.network = await self.get_network(self.network_id)
        self.network_name = self.network.name

        wss = getattr(self.network, f"wss_{self.provider}")

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
        self.network_id = options["network"]
        self.provider = options["provider"]
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
