import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional

import requests
from django.core.cache import cache
from django.core.management.base import BaseCommand
from sseclient import SSEClient

from utils.oracle import (
    parse_forwarder_call,
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

MEV_SHARE_MAINNET = os.getenv("MEVSHARE_ENDPOINT", "https://mev-share.flashbots.net")
FORWARDER_SELECTOR = "0x6fadcf72"  # same selector you keyed on in your WS version


class Command(BaseCommand):
    help = "Subscribe to Flashbots MEV-Share mainnet SSE and process forwarder calls"

    def add_arguments(self, parser):
        parser.add_argument(
            "--endpoint",
            default=MEV_SHARE_MAINNET,
            help="MEV-Share SSE endpoint (default: https://mev-share.flashbots.net)",
        )
        parser.add_argument(
            "--reconnect-delay",
            type=float,
            default=0.1,
            help="Seconds to wait before reconnecting after an error (default: 0.1)",
        )

    def handle(self, *args, **options):
        endpoint: str = options["endpoint"]
        reconnect_delay: float = options["reconnect_delay"]

        if not endpoint.startswith("http"):
            logger.error("Invalid MEV-Share endpoint: %s", endpoint)
            sys.exit(1)

        logger.info("Connecting to Flashbots MEV-Share SSE: %s", endpoint)

        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "User-Agent": "mev-share-django-listener/1.0",
        }

        while True:
            try:
                with requests.get(
                    endpoint, headers=headers, stream=True, timeout=90
                ) as resp:
                    resp.raise_for_status()
                    client = SSEClient(resp)
                    logger.info("Connected — listening for MEV-Share events")

                    for msg in client.events():
                        data = (msg.data or "").strip()
                        # Skip keepalive/comments
                        if not data or data == ":ping" or data.lower() == "ping":
                            continue
                        try:
                            event = json.loads(data)
                        except json.JSONDecodeError:
                            logger.warning("Non-JSON event chunk: %s", data[:200])
                            continue

                        try:
                            # file_context_0 indicates this was not being awaited!
                            # So: call the coroutine properly.
                            if hasattr(self, "process") and callable(
                                getattr(self, "process")
                            ):
                                # If async, run in sync context using asyncio.run or loop.create_task
                                import asyncio

                                if asyncio.iscoroutinefunction(self.process):
                                    asyncio.run(self.process(event))
                                else:
                                    self.process(event)
                            else:
                                logger.error("No 'process' method found on Command")
                        except Exception as e:
                            logger.error("Error processing MEV-Share event: %s", e)

            except KeyboardInterrupt:
                logger.info("Interrupted by user — exiting.")
                break
            except Exception as e:
                logger.error("Stream error: %s: %s", type(e).__name__, e)
                logger.info("Reconnecting in %s seconds…", reconnect_delay)
                time.sleep(reconnect_delay)

    async def process(self, response: Dict[str, Any]):
        txs = response.get("txs") or []
        if not txs:
            return

        for tx in txs:
            to_address = tx.get("to")
            allowed_transmitters = cache.get("transmitters_for_websockets")
            function_selector = tx.get("functionSelector")

            if (
                function_selector == FORWARDER_SELECTOR
                and to_address in allowed_transmitters
            ):
                calldata = tx.get("callData")
                logger.info(f"Processing forwarder call {calldata}")
                parsed_data = parse_forwarder_call(calldata)
                logger.info(tx)
                logger.info(parsed_data)
                # logger.info(f"Processing forwarder call: {tx.get('hash')}")
                # parsed_data = parse_forwarder_call(tx.get("input"))
                # await self.handle_oracle_update(parsed_data, tx)


def safe_lower(value: Optional[str]) -> Optional[str]:
    return value.lower() if isinstance(value, str) else value
