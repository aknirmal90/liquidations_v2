import logging
import math

from celery import Task

from aave.models import Asset
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
