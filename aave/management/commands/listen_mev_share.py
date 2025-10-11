import logging
from datetime import datetime, timezone

from django.core.management.base import BaseCommand

from aave.management.commands.listen_base import WebsocketCommand
from aave.tasks import UpdateAssetPriceTask, ProcessMevShareTransactionTask

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Command(WebsocketCommand, BaseCommand):
    help = "Subscribe to MEV Share pending transactions from Flashbots"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--mev-share-endpoint",
            action="store",
            default="wss://mev-share.flashbots.net",
            help="MEV Share websocket endpoint (default: wss://mev-share.flashbots.net)",
        )

    def get_subscribe_message(self):
        """
        Create subscription message for MEV Share.
        MEV Share uses different subscription format than standard eth_subscribe.
        """
        message = {
            "id": "1",
            "jsonrpc": "2.0", 
            "method": "mev_subscribe",
            "params": [
                "pending_transactions",
                {
                    "logs": [
                        {
                            "address": self.contract_addresses,
                            "topics": [
                                "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
                            ]
                        }
                    ]
                }
            ],
        }
        logger.debug(f"MEV Share subscribe message created: {message}")
        return message

    def parse_transaction_log(self, tx_data):
        """
        Parse transaction data from MEV Share.
        MEV Share returns different format than standard eth logs.
        """
        try:
            # MEV Share transaction format may include logs within transaction data
            if "logs" in tx_data:
                for log in tx_data["logs"]:
                    if (log.get("topics") and 
                        len(log["topics"]) > 0 and 
                        log["topics"][0] == "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"):
                        
                        return {
                            "asset": log["address"],
                            "new_price": int(log["topics"][1], 16) if len(log["topics"]) > 1 else 0,
                            "block_height": int(tx_data.get("blockNumber", "0x0"), 16),
                            "updated_at": int(log.get("data", "0x0"), 16),
                            "roundId": int(log["topics"][2], 16) if len(log["topics"]) > 2 else 0,
                            "transaction_hash": tx_data.get("hash", ""),
                        }
            
            # Fallback: if direct log access in transaction
            elif "topics" in tx_data and len(tx_data["topics"]) > 0:
                if tx_data["topics"][0] == "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f":
                    return {
                        "asset": tx_data.get("address", ""),
                        "new_price": int(tx_data["topics"][1], 16) if len(tx_data["topics"]) > 1 else 0,
                        "block_height": int(tx_data.get("blockNumber", "0x0"), 16),
                        "updated_at": int(tx_data.get("data", "0x0"), 16),
                        "roundId": int(tx_data["topics"][2], 16) if len(tx_data["topics"]) > 2 else 0,
                        "transaction_hash": tx_data.get("hash", ""),
                    }
                    
        except (ValueError, KeyError, IndexError) as e:
            logger.error(f"Error parsing MEV Share transaction data: {e}")
            logger.debug(f"Transaction data: {tx_data}")
            
        return None

    async def process(self, msg, **kwargs):
        """Process MEV Share message and extract relevant data."""
        onchain_received_at = datetime.now(timezone.utc)
        
        logger.debug(f"Processing MEV Share message: {msg}")

        # MEV Share message structure may be different
        if not ("params" in msg and "result" in msg["params"]):
            logger.debug("Message doesn't contain expected params.result structure")
            return

        tx_data = msg["params"]["result"]
        
        # Parse the transaction data to extract log information  
        parsed_log = self.parse_transaction_log(tx_data)
        
        if not parsed_log:
            logger.debug("No relevant log data found in transaction")
            return
            
        logger.info(f"Found relevant MEV Share transaction: {parsed_log['transaction_hash']}")
        
        # Check if this is a new price update
        is_new_price = self.check_and_update_price_cache(
            new_price=parsed_log["new_price"],
            asset=parsed_log["asset"]
        )
        
        if not is_new_price:
            logger.debug(f"Price not changed for asset {parsed_log['asset']}")
            return

        processed_at = datetime.now(timezone.utc)
        
        logger.info(f"Processing MEV Share price update: {parsed_log['asset']} -> {parsed_log['new_price']}")

        # Use the same task as the regular price listener
        UpdateAssetPriceTask.apply_async(
            kwargs={
                "network_id": self.network.id,
                "network_name": self.network.name,
                "contract": parsed_log['asset'],
                "new_price": parsed_log['new_price'],
                "onchain_created_at": parsed_log['updated_at'],
                "round_id": parsed_log['roundId'],
                "onchain_received_at": onchain_received_at,
                "provider": f"mev-share-{self.provider}",  # Distinguish MEV Share source
                "transaction_hash": parsed_log['transaction_hash'],
                "processed_at": processed_at
            },
            priority=1  # Slightly lower priority than direct blockchain data
        )

        # Also process MEV-specific data
        ProcessMevShareTransactionTask.apply_async(
            kwargs={
                "transaction_hash": parsed_log['transaction_hash'],
                "asset_address": parsed_log['asset'],
                "network_id": self.network.id,
                "network_name": self.network.name,
                "price": parsed_log['new_price'],
                "round_id": parsed_log['roundId'],
                "block_height": parsed_log.get('block_height', 0),
                "mev_received_at": onchain_received_at,
                "onchain_created_at": parsed_log['updated_at'],
                "processed_at": processed_at,
                "raw_transaction_data": tx_data  # Store raw data for analysis
            },
            priority=2  # Lower priority than price updates
        )

    async def listen(self):
        """Override listen method to use MEV Share endpoint."""
        # Use the MEV Share endpoint instead of network's websocket
        mev_share_endpoint = getattr(self, 'mev_share_endpoint', 'wss://mev-share.flashbots.net')
        logger.info(f"Connecting to MEV Share endpoint: {mev_share_endpoint}")

        # Pre-compute subscribe message once
        from asgiref.sync import sync_to_async
        import orjson as json
        
        subscribe_message = await sync_to_async(
            self.get_subscribe_message, thread_sensitive=True
        )()
        subscribe_message_json = json.dumps(subscribe_message)
        logger.info(f"MEV Share subscription: {subscribe_message_json}")

        # Import websockets here to match the parent class pattern
        import websockets
        import asyncio

        # Reconnection loop
        while True:
            try:
                # Connect to MEV Share with optimized settings
                async with websockets.connect(
                    mev_share_endpoint,
                    ping_interval=None,  # Disable ping/pong
                    max_size=2**24,  # Increase max message size
                    compression=None,  # Disable compression
                ) as websocket:
                    await websocket.send(subscribe_message_json)
                    logger.info("Successfully subscribed to MEV Share")

                    # Process messages as fast as possible
                    while True:
                        try:
                            msg = json.loads(await websocket.recv())
                            logger.debug(f"Received MEV Share message: {msg}")
                            
                            if "params" not in msg:
                                continue

                            # Process message without awaiting to reduce latency
                            asyncio.create_task(self.process(msg))
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to decode JSON message: {e}")
                            continue

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"MEV Share connection closed: {e}. Reconnecting...")
                await asyncio.sleep(1)  # Slightly longer delay for MEV Share
            except Exception as e:
                logger.error(f"Error in MEV Share connection: {e}")
                await asyncio.sleep(5)  # Wait before retrying on general errors
                continue

    def handle(self, *args, **options):
        """Handle command execution with MEV Share specific setup."""
        self.network_name = options["network"]
        self.network = self.get_network_by_name(self.network_name)
        self.provider = options.get("provider", "mev-share")
        self.mev_share_endpoint = options.get("mev_share_endpoint", "wss://mev-share.flashbots.net")
        self.contract_addresses = self.get_contract_addresses()

        # Use uvloop if available for better performance
        import asyncio
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass

        asyncio.run(self.listen())

    def get_network_by_name(self, network_name):
        """Get network instance by name."""
        from blockchains.models import Network
        return Network.objects.get(name=network_name)