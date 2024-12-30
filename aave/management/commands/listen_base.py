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

    async def get_network(self, network_id):
        instance = await sync_to_async(Network.objects.get, thread_sensitive=True)(
            name=network_id
        )
        return instance

    async def listen(self):
        self.network = await self.get_network(self.network_id)
        wss = self.network.wss

        # Reconnection loop
        while True:
            try:
                # Attempt to connect
                async with websockets.connect(wss) as websocket:
                    subscribe_message = await sync_to_async(
                        self.get_subscribe_message, thread_sensitive=True
                    )()
                    await websocket.send(
                        json.dumps(subscribe_message)
                    )
                    logger.info(json.dumps(subscribe_message))

                    while True:
                        response = await websocket.recv()
                        msg = json.loads(response)

                        if "params" not in msg:
                            logger.info(msg)
                            continue

                        await self.process(msg)
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed with error: {e}. Reconnecting...")
                await asyncio.sleep(1)  # Wait a bit before trying to reconnect
            except Exception as e:
                logger.error(f"Error in connection: {e}")
                break  # Break out of the loop if an unexpected error occurs

    def handle(self, *args, **options):

        self.network_id = options["network"]
        asyncio.run(self.listen())

    def get_subscribe_message(self):
        raise NotImplementedError

    async def process(self, response):
        raise NotImplementedError
