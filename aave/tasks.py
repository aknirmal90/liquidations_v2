import logging
import math
from datetime import datetime, timezone
from decimal import Decimal

from celery import Task
from web3 import Web3

from aave.models import Asset, AssetPriceLog
from liquidations_v2.celery_app import app
from utils.tokens import EvmTokenRetriever

logger = logging.getLogger(__name__)


class ResetAssetsTask(Task):
    """Task to reset all Asset model instances and AssetPriceLog instances."""

    def run(self):
        """Delete all Asset and AssetPriceLog model instances."""
        logger.info("Starting ResetAssetsTask")

        price_log_count = AssetPriceLog.objects.count()
        AssetPriceLog.objects.all().delete()
        logger.info(f"Successfully deleted {price_log_count} AssetPriceLog instances")

        asset_count = Asset.objects.count()
        Asset.objects.all().delete()
        logger.info(f"Successfully deleted {asset_count} Aave Asset instances")

        logger.info("Completed ResetAssetsTask")


ResetAssetsTask = app.register_task(ResetAssetsTask())


class UpdateAssetMetadataTask(Task):
    """Task to update token metadata for assets with missing symbols."""

    def run(self):
        """Update metadata for assets with null/blank symbols."""
        logger.info("Starting UpdateAssetMetadataTask")

        assets = Asset.objects.filter(symbol__isnull=True) | Asset.objects.filter(symbol='')
        count = assets.count()

        if count == 0:
            logger.info("No assets found requiring metadata update")
            return

        logger.info(f"Found {count} assets requiring metadata update")

        for asset in assets:
            try:
                token_retriever = EvmTokenRetriever(
                    network_name=asset.network.name,
                    token_address=asset.asset
                )
                asset.symbol = token_retriever.name
                asset.num_decimals = token_retriever.num_decimals
                asset.decimals = math.pow(10, token_retriever.num_decimals)
                asset.save(update_fields=['symbol', 'num_decimals', 'decimals'])
                logger.info(f"Updated metadata for asset {asset.asset}")
            except Exception as e:
                logger.error(f"Failed to update metadata for asset {asset.asset}: {str(e)}")

        logger.info("Completed UpdateAssetMetadataTask")


UpdateAssetMetadataTask = app.register_task(UpdateAssetMetadataTask())


class UpdateAssetPriceTask(Task):
    """Task to update asset prices and create price logs."""

    def run(
        self,
        network_id,
        contract,
        new_price,
        block_height,
        onchain_created_at,
        round_id,
        onchain_received_at,
        provider
    ):
        assets_to_update = []

        # Update assets where contract matches contractA
        assetsA = Asset.objects.filter(contractA__iexact=contract)
        assetsA.update(
            priceA=Decimal(new_price),
            updated_at_block_heightA=block_height
        )
        for asset in assetsA:
            try:
                price, price_in_usdt = asset.get_price()
                asset.price = price
                asset.price_in_usdt = price_in_usdt
                assets_to_update.append(asset)
            except Exception as e:
                logger.error(f"Failed to get price for asset {asset.asset}: {str(e)}")
                continue

        # Update assets where contract matches contractB
        assetsB = Asset.objects.filter(contractB__iexact=contract)
        assetsB.update(
            priceB=Decimal(new_price),
            updated_at_block_heightB=block_height
        )
        for asset in assetsB:
            try:
                price, price_in_usdt = asset.get_price()
                asset.price = price
                asset.price_in_usdt = price_in_usdt
                assets_to_update.append(asset)
            except Exception as e:
                logger.error(f"Failed to get price for asset {asset.asset}: {str(e)}")
                continue

        # Bulk save all updated assets
        Asset.objects.bulk_update(assets_to_update, ['price', 'price_in_usdt'])

        AssetPriceLog.objects.create(
            aggregator_address=contract,
            network_id=network_id,
            price=new_price,
            onchain_created_at=datetime.fromtimestamp(onchain_created_at, tz=timezone.utc),
            round_id=round_id,
            onchain_received_at=onchain_received_at,
            provider=provider
        )


UpdateAssetPriceTask = app.register_task(UpdateAssetPriceTask())


class UpdateMaxCappedRatiosTask(Task):
    """Task to update max capped ratios for assets."""

    def run(self):
        """Update max capped ratios for assets."""
        assets = Asset.objects.filter(price_type=Asset.PriceType.RATIO)
        functions = [
            ("MINIMUM_SNAPSHOT_DELAY", "uint48"),
            ("getMaxRatioGrowthPerSecond", "uint256"),
            ("getSnapshotRatio", "uint256"),
            ("getSnapshotTimestamp", "uint256")
        ]
        abi = []
        for func_name, return_type in functions:
            abi.append({
                "inputs": [],
                "name": func_name,
                "outputs": [{"type": return_type}],
                "stateMutability": "view",
                "type": "function"
            })
        for asset in assets:
            try:
                w3 = Web3(Web3.HTTPProvider(asset.network.rpc))
                contract = w3.eth.contract(address=Web3.to_checksum_address(asset.pricesource), abi=abi)

                results = {}
                for func_name, _ in functions:
                    try:
                        result = contract.functions[func_name]().call()
                        results[func_name] = result
                    except Exception as e:
                        logger.error(f"Failed to call {func_name} for asset {asset.asset}: {str(e)}")
                        continue

                current_ts = int(datetime.now(timezone.utc).timestamp())
                current_delay = current_ts - results['getSnapshotTimestamp']
                if current_delay < results['MINIMUM_SNAPSHOT_DELAY']:
                    continue

                max_cap = results['getSnapshotRatio'] + results['getMaxRatioGrowthPerSecond'] * current_delay
                asset.max_cap = max_cap
                asset.save(update_fields=['max_cap'])

                logger.info(f"Got results for asset {asset.asset}: {results}")

            except Exception as e:
                logger.error(f"Failed to process asset {asset.asset}: {str(e)}")
                continue


UpdateMaxCappedRatiosTask = app.register_task(UpdateMaxCappedRatiosTask())
