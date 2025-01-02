import logging
import math
from datetime import datetime, timezone
from decimal import Decimal

from celery import Task
from web3 import Web3

from aave.models import AaveLiquidationLog, Asset, AssetPriceLog
from liquidations_v2.celery_app import app
from utils.simulation import get_simulated_health_factor
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
        provider,
        onchain_received_at,
        transaction_hash,
        onchain_created_at=None,
        round_id=None
    ):
        assets_to_update = []

        # Update assets where contract matches contractA
        assetsA = Asset.objects.filter(contractA__iexact=contract)
        assetsA.update(
            priceA=Decimal(new_price),
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

        if onchain_created_at:
            onchain_created_at = datetime.fromtimestamp(onchain_created_at, tz=timezone.utc)

        AssetPriceLog.objects.create(
            aggregator_address=contract,
            network_id=network_id,
            price=new_price,
            onchain_created_at=onchain_created_at,
            round_id=round_id,
            onchain_received_at=onchain_received_at,
            provider=provider,
            transaction_hash=transaction_hash
        )
        if not provider.startswith("sequencer"):
            AssetPriceLog.objects.filter(
                transaction_hash=transaction_hash,
                network_id=network_id
            ).update(
                onchain_created_at=onchain_created_at,
                round_id=round_id
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


class UpdateSimulatedHealthFactorTask(Task):
    """Task to update simulated health factor for aave liquidation logs."""

    def run(self):
        """Update simulated health factor for aave liquidation logs."""
        liquidation_logs = (
            AaveLiquidationLog.objects
            .filter(health_factor_before_tx__isnull=True)
            .order_by('-block_height')[:200]
        )

        total_logs = len(liquidation_logs)
        logger.info(f"Processing {total_logs} liquidation logs")

        for idx, liquidation_log in enumerate(liquidation_logs, 1):

            params = [
                # before tx
                (liquidation_log.block_height,
                 liquidation_log.transaction_index - 1,
                 "health_factor_before_tx"),
                # before tx and zero blocks
                (liquidation_log.block_height,
                 1,
                 "health_factor_before_zero_blocks"),
                # before tx and one block
                (liquidation_log.block_height - 1,
                 1,
                 "health_factor_before_one_blocks"),
                # before tx and two blocks
                (liquidation_log.block_height - 2,
                 1,
                 "health_factor_before_two_blocks"),
                # before tx and three blocks
                (liquidation_log.block_height - 3,
                 1,
                 "health_factor_before_three_blocks"),
            ]

            logger.info(
                f"Processing liquidation log {liquidation_log.id} for user {liquidation_log.user} ({idx}/{total_logs})"
            )
            try:
                for param in params:
                    logger.debug(f"Getting health factor at block {param[0]} tx index {param[1]}")
                    health_factor = get_simulated_health_factor(
                        chain_id=liquidation_log.network.chain_id,
                        address=liquidation_log.user,
                        block_number=param[0],
                        transaction_index=param[1]
                    )
                    logger.info(f"Got health factor {health_factor} for {param[2]}")
                    setattr(liquidation_log, param[2], health_factor)
                liquidation_log.save()
                logger.info(f"Successfully updated health factors for liquidation log {liquidation_log.id}")
            except Exception as e:
                logger.error(
                    f"Failed to process liquidation log {liquidation_log.id}: {str(e)}",
                    exc_info=True
                )
                continue

        logger.info(f"Completed processing {total_logs} liquidation logs")


UpdateSimulatedHealthFactorTask = app.register_task(UpdateSimulatedHealthFactorTask())
