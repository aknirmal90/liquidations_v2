import logging
import math
from datetime import datetime, timezone
from decimal import Decimal

from celery import Task

from aave.models import Asset, AssetPriceLog
from liquidations_v2.celery_app import app
from utils.tokens import EvmTokenRetriever

logger = logging.getLogger(__name__)


class ResetAssetsTask(Task):
    """Task to reset all Asset model instances."""

    def run(self):
        """Delete all Asset model instances."""
        logger.info("Starting ResetAssetsTask")
        count = Asset.objects.count()
        Asset.objects.all().delete()
        logger.info(f"Successfully deleted {count} Aave Asset instances")
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
        onchain_received_at
    ):
        # Update assets where contract matches contractA
        # Update assets where contract matches contractA
        Asset.objects.filter(contractA__iexact=contract).update(
            priceA=Decimal(new_price),
            updated_at_block_heightA=block_height
        )

        # Update assets where contract matches contractB
        Asset.objects.filter(contractB__iexact=contract).update(
            priceB=Decimal(new_price),
            updated_at_block_heightB=block_height
        )

        AssetPriceLog.objects.create(
            aggregator_address=contract,
            network_id=network_id,
            price=new_price,
            onchain_created_at=datetime.fromtimestamp(onchain_created_at, tz=timezone.utc),
            round_id=round_id,
            onchain_received_at=onchain_received_at
        )


UpdateAssetPriceTask = app.register_task(UpdateAssetPriceTask())
